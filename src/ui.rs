//! Ratatui rendering pipeline: header (context · namespace · user · read-only),
//! generic resource table, YAML/detail viewer, dry-run diff viewer, explain view,
//! modal pickers/palettes, and status bar.

use ratatui::Frame;
use ratatui::backend::TestBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, TableState,
    Wrap,
};
use unicode_width::UnicodeWidthStr;

use crate::app::{App, ExplainFinding, FindingSeverity, Mode};
use crate::columns::ColumnKind;
use crate::store::PollFailure;
use crate::theme;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const HEADER_LOGO_WIDTH: u16 = 22;

/// Render the entire application frame.
pub fn draw(f: &mut Frame, app: &App) {
    let size = f.area();
    if let Some(bg) = theme::background() {
        let bg_block = Block::default().style(Style::default().bg(bg));
        f.render_widget(bg_block, size);
    }

    let show_filter_bar = matches!(app.mode, Mode::Filter { .. }) || !app.filter_text.is_empty();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if show_filter_bar {
            vec![
                Constraint::Length(7),
                Constraint::Length(3),
                Constraint::Min(5),
                Constraint::Length(1),
            ]
        } else {
            vec![
                Constraint::Length(7),
                Constraint::Min(5),
                Constraint::Length(1),
            ]
        })
        .split(size);

    draw_header(f, app, chunks[0]);

    let (body_area, status_area) = if show_filter_bar {
        draw_filter_bar(f, app, chunks[1]);
        (chunks[2], chunks[3])
    } else {
        (chunks[1], chunks[2])
    };

    match &app.mode {
        Mode::Detail {
            title,
            lines,
            scroll,
            hscroll,
            wrap,
            editable,
        } => {
            draw_detail(
                f,
                body_area,
                title,
                lines,
                *scroll,
                *hscroll,
                *wrap,
                editable.is_some() && !app.is_readonly(),
            );
        }
        Mode::Diff {
            title,
            summary,
            diff_lines,
            scroll,
            ..
        } => {
            draw_diff(f, body_area, title, summary, diff_lines, *scroll);
        }
        Mode::Explain {
            title,
            findings,
            scroll,
            wrap,
        } => {
            draw_explain(f, body_area, title, findings, *scroll, *wrap);
        }
        Mode::Help { scroll } => {
            draw_help(f, body_area, *scroll);
        }
        _ => {
            draw_table(f, app, body_area);
        }
    }

    // Render modal overlays on top of the table
    match &app.mode {
        Mode::Command { input, selected } => {
            draw_command_popup(f, app, size, input, *selected);
        }
        Mode::Namespaces { filter, selected } => {
            draw_namespaces_popup(f, app, size, filter, *selected);
        }
        Mode::Contexts { filter, selected } => {
            draw_contexts_popup(f, app, size, filter, *selected);
        }
        Mode::SortPicker { selected } => {
            draw_sort_picker_popup(f, app, size, *selected);
        }
        Mode::ActionMenu {
            title,
            entries,
            selected,
        } => {
            draw_action_menu_popup(f, app, size, title, entries, *selected);
        }
        Mode::Confirm { title, summary, .. } => {
            draw_confirm_popup(f, size, title, summary);
        }
        Mode::Prompt {
            title,
            prompt,
            input,
            ..
        } => {
            draw_prompt_popup(f, size, title, prompt, input);
        }
        Mode::Skins { selected, .. } => {
            draw_skins_popup(f, size, *selected);
        }
        _ => {}
    }

    draw_status_bar(f, app, status_area);
}

fn header_title(server_version: Option<&str>) -> Line<'static> {
    let mut spans = vec![Span::styled(" domite ", theme::title())];
    if let Some(ver) = server_version
        && !ver.is_empty()
    {
        spans.push(Span::styled("· ns4kafka Rev: ", theme::dim()));
        spans.push(Span::styled(
            ver.to_string(),
            Style::default().fg(theme::sapphire()),
        ));
        spans.push(Span::raw(" "));
    }
    Line::from(spans)
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let show_logo = area.width >= 60;
    let logo_width = if show_logo { HEADER_LOGO_WIDTH } else { 0 };
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(30), Constraint::Length(logo_width)])
        .split(area);

    let ns_label = app
        .active_namespace
        .as_deref()
        .map(|n| if n == "*" { "<all>" } else { n })
        .unwrap_or("<all>");
    let user_label = app.username.as_deref().unwrap_or("-");
    let api_url = crate::redact::credentials(&app.client.context().api);

    let field = |label: &str, val: String, color| {
        Line::from(vec![
            Span::styled(format!(" {label:<12}"), theme::dim()),
            Span::styled(val, Style::default().fg(color)),
        ])
    };

    let mut context_line = field("Context:", app.active_context.clone(), theme::mauve());
    if app.is_readonly() {
        context_line.push_span(Span::styled(
            "  [read-only]",
            Style::default()
                .fg(theme::red())
                .add_modifier(Modifier::BOLD),
        ));
    }

    let info_lines = vec![
        context_line,
        field("Cluster:", api_url.into_owned(), theme::sapphire()),
        field("Namespace:", ns_label.to_string(), theme::green()),
        field("User:", user_label.to_string(), theme::peach()),
        field(
            "Resource:",
            format!("{} ({})", app.active_resource.kind, app.store.len()),
            theme::text(),
        ),
    ];

    let left_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(theme::border())
        .title(header_title(app.server_version.as_deref()));
    let inner = left_block.inner(cols[0]);
    f.render_widget(left_block, cols[0]);
    f.render_widget(Paragraph::new(info_lines), inner);

    if show_logo {
        let dome_rock = Style::default().fg(theme::overlay1());
        let lava_hot = Style::default()
            .fg(theme::red())
            .add_modifier(Modifier::BOLD);
        let lava_warm = Style::default()
            .fg(theme::peach())
            .add_modifier(Modifier::BOLD);
        let lava_spark = Style::default()
            .fg(theme::yellow())
            .add_modifier(Modifier::BOLD);

        // Puy de Dôme volcano with glowing lava & sparks; each line is 18 chars wide
        let logo = vec![
            Line::from(vec![
                Span::styled("     ( ", dome_rock),
                Span::styled(".*", lava_spark),
                Span::styled(" ", dome_rock),
                Span::styled("*.", lava_warm),
                Span::styled(" )    ", dome_rock),
            ]),
            Line::from(vec![
                Span::styled("      ) ", dome_rock),
                Span::styled("~ ~", lava_hot),
                Span::styled(" (     ", dome_rock),
            ]),
            Line::from(vec![
                Span::styled("     /", dome_rock),
                Span::styled(" ~~~~~ ", lava_hot),
                Span::styled("\\    ", dome_rock),
            ]),
            Line::from(vec![
                Span::styled("    /  ", dome_rock),
                Span::styled("\\", lava_warm),
                Span::styled("~~~", lava_hot),
                Span::styled("/", lava_warm),
                Span::styled("  \\   ", dome_rock),
            ]),
            Line::from(vec![
                Span::styled("   /     ", dome_rock),
                Span::styled("V", lava_spark),
                Span::styled("     \\  ", dome_rock),
            ]),
            Line::from(Span::styled("  /_____________\\ ", dome_rock)),
            Line::from(Span::styled(
                format!("   domite v{VERSION:<6}"),
                theme::dim(),
            )),
        ];
        f.render_widget(Paragraph::new(logo).alignment(Alignment::Right), cols[1]);
    }
}

fn draw_filter_bar(f: &mut Frame, app: &App, area: Rect) {
    let (text, focused) = match &app.mode {
        Mode::Filter { input } => (input.as_str(), true),
        _ => (app.filter_text.as_str(), false),
    };
    let border_style = if focused {
        theme::border_focused()
    } else {
        theme::border()
    };
    let mut spans = vec![
        Span::styled(
            " /",
            Style::default()
                .fg(theme::yellow())
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(text, Style::default().fg(theme::text())),
    ];
    if focused {
        spans.push(Span::styled("█", Style::default().fg(theme::lavender())));
    }
    if let Some(err) = app.filter_parsed.error() {
        spans.push(Span::raw("   "));
        spans.push(Span::styled(
            format!("({err})"),
            Style::default().fg(theme::red()),
        ));
    }
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(Span::styled(" Filter ", theme::title()));
    f.render_widget(Paragraph::new(Line::from(spans)).block(block), area);
}

fn draw_table(f: &mut Frame, app: &App, area: Rect) {
    let ns_display = app
        .active_namespace
        .as_deref()
        .map(|n| if n == "*" { "all" } else { n })
        .unwrap_or("all");
    let count_str = if app.filter_text.is_empty() {
        format!("{}", app.rows.len())
    } else {
        format!("{}/{}", app.rows.len(), app.store.len())
    };
    let mut title_spans = vec![
        Span::styled(
            format!(" {}({})", app.active_resource.kind, ns_display),
            theme::title(),
        ),
        Span::styled(
            format!("[{count_str}]"),
            Style::default()
                .fg(theme::counter())
                .add_modifier(Modifier::BOLD),
        ),
    ];
    if !app.marked.is_empty() {
        title_spans.push(Span::styled(
            format!(" [{} marked]", app.marked.len()),
            Style::default()
                .fg(theme::mark())
                .add_modifier(Modifier::BOLD),
        ));
    }
    if app.wide {
        title_spans.push(Span::styled(" [wide]", theme::dim()));
    }
    if let Some((done, total)) = app.hydrating_progress() {
        title_spans.push(Span::styled(
            format!(" [hydrating {done}/{total}…]"),
            Style::default().fg(theme::sapphire()),
        ));
    }
    if !app.store.synced && app.poll_error.is_none() {
        title_spans.push(Span::styled(" [polling…] ", theme::dim()));
    } else {
        title_spans.push(Span::raw(" "));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Line::from(title_spans));

    // Handle 403 Forbidden gracefully with a clear explanation inside the table block
    if let Some((err, PollFailure::Forbidden)) = &app.poll_error
        && app.rows.is_empty()
    {
        let msg = vec![
            Line::from(""),
            Line::from(Span::styled(
                "  403 Forbidden — Insufficient permissions for this scope",
                Style::default()
                    .fg(theme::red())
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(
                format!("  {err}"),
                Style::default().fg(theme::subtext1()),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "  Press 'n' to switch to an authorized namespace, or ':ctx' to switch context.",
                theme::dim(),
            )),
        ];
        f.render_widget(Paragraph::new(msg).block(block), area);
        return;
    }

    let header_cells: Vec<Cell> = app
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let mut label = col.header.clone();
            if let Some((s_idx, desc)) = app.sort_col
                && s_idx == idx
            {
                label.push_str(if desc { " ▼" } else { " ▲" });
                return Cell::from(Line::from(vec![
                    Span::styled(col.header.clone(), theme::header_row()),
                    Span::styled(
                        if desc { " ▼" } else { " ▲" },
                        Style::default()
                            .fg(theme::sorter())
                            .add_modifier(Modifier::BOLD),
                    ),
                ]));
            }
            Cell::from(label).style(theme::header_row())
        })
        .collect();
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = app
        .rows
        .iter()
        .enumerate()
        .map(|(row_idx, r)| {
            let is_selected = row_idx == app.cursor;
            let is_marked = app.marked.contains(&r.key);
            let base_fg = if is_marked {
                theme::mark()
            } else {
                theme::row_color(&r.status)
            };

            let cells: Vec<Cell> = r
                .cells
                .iter()
                .enumerate()
                .map(|(c_idx, text)| {
                    let display = if c_idx == 0 && is_marked {
                        format!("* {text}")
                    } else {
                        text.clone()
                    };
                    let col_kind = app
                        .columns
                        .get(c_idx)
                        .map(|c| c.kind)
                        .unwrap_or(ColumnKind::Text);
                    let fg = if col_kind == ColumnKind::Status && !is_selected {
                        theme::status_color(text)
                    } else {
                        base_fg
                    };
                    Cell::from(display).style(Style::default().fg(fg))
                })
                .collect();

            let row_style = if is_selected && is_marked {
                theme::selected_marked_row()
            } else if is_selected {
                theme::selected_row()
            } else {
                Style::default()
            };
            Row::new(cells).style(row_style)
        })
        .collect();

    let widths = compute_column_widths(app, area.width.saturating_sub(4));
    let mut state = TableState::default();
    if !app.rows.is_empty() {
        state.select(Some(app.cursor));
    }

    let table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .column_spacing(2);
    f.render_stateful_widget(table, area, &mut state);
}

fn compute_column_widths(app: &App, available: u16) -> Vec<Constraint> {
    if app.columns.is_empty() {
        return Vec::new();
    }
    let n = app.columns.len();
    let mut max_lens: Vec<u16> = app
        .columns
        .iter()
        .map(|c| (c.header.width() as u16) + 2)
        .collect();
    for r in app.rows.iter().take(100) {
        for (i, cell) in r.cells.iter().enumerate() {
            if i < max_lens.len() {
                let w = (cell.width() as u16) + if i == 0 { 3 } else { 1 };
                if w > max_lens[i] {
                    max_lens[i] = w.min(48);
                }
            }
        }
    }
    let total: u16 = max_lens.iter().sum::<u16>() + (n.saturating_sub(1) as u16 * 2);
    if total <= available {
        max_lens
            .into_iter()
            .enumerate()
            .map(|(i, w)| {
                if i == 0 {
                    Constraint::Min(w)
                } else {
                    Constraint::Length(w)
                }
            })
            .collect()
    } else {
        max_lens
            .into_iter()
            .map(|w| Constraint::Length(w.min(36)))
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
fn draw_detail(
    f: &mut Frame,
    area: Rect,
    title: &str,
    lines: &[String],
    scroll: usize,
    hscroll: usize,
    wrap: bool,
    editable: bool,
) {
    let hint = if editable {
        " <q/Esc> back · <j/k> scroll · <w> wrap · <e> edit in $EDITOR "
    } else {
        " <q/Esc> back · <j/k> scroll · <w> wrap "
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(format!(" {title} "), theme::title()))
        .title_bottom(Span::styled(hint, theme::dim()));

    let visible: Vec<Line> = lines
        .iter()
        .skip(scroll)
        .map(|line| {
            let sliced: String = if hscroll > 0 {
                line.chars().skip(hscroll).collect()
            } else {
                line.clone()
            };
            style_yaml_line(&sliced)
        })
        .collect();

    let mut para = Paragraph::new(visible).block(block);
    if wrap {
        para = para.wrap(Wrap { trim: false });
    }
    f.render_widget(para, area);
}

fn style_yaml_line(line: &str) -> Line<'static> {
    if line.trim_start().starts_with('#') {
        return Line::from(Span::styled(
            line.to_string(),
            Style::default()
                .fg(theme::peach())
                .add_modifier(Modifier::BOLD),
        ));
    }
    if let Some((key, rest)) = line.split_once(':') {
        return Line::from(vec![
            Span::styled(format!("{key}:"), Style::default().fg(theme::sky())),
            Span::styled(rest.to_string(), Style::default().fg(theme::text())),
        ]);
    }
    Line::from(Span::styled(
        line.to_string(),
        Style::default().fg(theme::text()),
    ))
}

fn draw_diff(
    f: &mut Frame,
    area: Rect,
    title: &str,
    summary: &[String],
    diff_lines: &[String],
    scroll: usize,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(format!(" {title} "), theme::title()))
        .title_bottom(Span::styled(
            " <y/Enter> confirm & execute · <n/Esc> cancel · <j/k> scroll ",
            Style::default()
                .fg(theme::yellow())
                .add_modifier(Modifier::BOLD),
        ));

    let mut rendered = Vec::new();
    for s in summary {
        rendered.push(Line::from(Span::styled(
            s.clone(),
            Style::default()
                .fg(theme::yellow())
                .add_modifier(Modifier::BOLD),
        )));
    }
    if !summary.is_empty() {
        rendered.push(Line::from(""));
    }
    for l in diff_lines.iter().skip(scroll) {
        let style = if l.starts_with('+') {
            Style::default().fg(theme::green())
        } else if l.starts_with('-') {
            Style::default().fg(theme::red())
        } else {
            Style::default().fg(theme::subtext1())
        };
        rendered.push(Line::from(Span::styled(l.clone(), style)));
    }
    f.render_widget(Paragraph::new(rendered).block(block), area);
}

fn draw_explain(
    f: &mut Frame,
    area: Rect,
    title: &str,
    findings: &[ExplainFinding],
    scroll: usize,
    wrap: bool,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(format!(" {title} "), theme::title()))
        .title_bottom(Span::styled(
            " <q/Esc> back · <j/k> scroll · <w> wrap ",
            theme::dim(),
        ));

    let mut lines = Vec::new();
    for finding in findings.iter().skip(scroll) {
        let (badge, color) = match finding.severity {
            FindingSeverity::Error => ("[FAIL] ", theme::red()),
            FindingSeverity::Warning => ("[WARN] ", theme::yellow()),
            FindingSeverity::Ok => ("[OK]   ", theme::green()),
        };
        lines.push(Line::from(vec![
            Span::styled(
                badge,
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                finding.heading.clone(),
                Style::default()
                    .fg(theme::text())
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
        for d in &finding.detail {
            lines.push(Line::from(Span::styled(
                format!("       {d}"),
                Style::default().fg(theme::subtext1()),
            )));
        }
        lines.push(Line::from(""));
    }

    let mut para = Paragraph::new(lines).block(block);
    if wrap {
        para = para.wrap(Wrap { trim: false });
    }
    f.render_widget(para, area);
}

fn draw_help(f: &mut Frame, area: Rect, scroll: usize) {
    let help_lines = vec![
        (
            "Navigation & Views",
            vec![
                ("j / k / ↑ / ↓", "Move cursor down / up"),
                ("g / G / Home / End", "Jump to first / last row"),
                (
                    "Enter",
                    "Drill down (Namespace → Topics, Topic → ConsumerGroups, ConnectCluster → Connectors)",
                ),
                (
                    "Esc / [ / ]",
                    "Clear filter / selection, or navigate back / forward in history",
                ),
                (
                    ":",
                    "Open command palette (topics, connectors, schemas, acls, cg, streams, quotas, cc, rb, ns)",
                ),
                (
                    "/",
                    "Filter current table (supports text, a|b, ~fuzzy, /regex/, !not, status=Fail, lag>0, age<2h)",
                ),
                (
                    "n / 0",
                    "Open namespace switcher / view all namespaces (admin)",
                ),
                ("C / :ctx", "Open kafkactl context switcher"),
                ("K / :skin", "Switch color skin (17 built-in themes)"),
                ("w", "Toggle wide columns"),
                (
                    "S / I / A",
                    "Open sort column picker / invert sort / sort by AGE",
                ),
                ("r / Ctrl-r", "Force immediate poll refresh"),
            ],
        ),
        (
            "Resource Inspection & Safe Writes",
            vec![
                (
                    "y / d",
                    "View resource YAML (with sensitive credentials redacted)",
                ),
                (
                    "X",
                    "Explain why a Topic or Connector is unhealthy (shows failed task stack traces)",
                ),
                (
                    "t",
                    "Open kind-specific action menu (pause/resume/restart connector, reset offsets, vault, etc.)",
                ),
                (
                    "Space",
                    "Mark / unmark rows for bulk delete or bulk connector state actions",
                ),
                (
                    "e",
                    "Edit resource in $EDITOR → server dry-run validation → unified diff preview → confirm",
                ),
                (
                    "Ctrl-d",
                    "Delete selected/marked resource(s) after server dry-run preview",
                ),
                (
                    "i",
                    "Import unsynchronized topics or connectors (POST /_/import with dry-run preview)",
                ),
                (
                    ":readonly / :write",
                    "Toggle session read-only guardrail (prod* contexts default to read-only)",
                ),
                ("q / Ctrl-c", "Close overlay or quit domite"),
            ],
        ),
    ];

    let mut lines = Vec::new();
    for (section, items) in help_lines {
        lines.push(Line::from(Span::styled(
            format!("── {section} ──"),
            theme::title(),
        )));
        for (k, desc) in items {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {k:<22}"),
                    Style::default()
                        .fg(theme::yellow())
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(desc, Style::default().fg(theme::text())),
            ]));
        }
        lines.push(Line::from(""));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(" Help · domite ", theme::title()))
        .title_bottom(Span::styled(
            " <q/Esc/?> close · <j/k> scroll ",
            theme::dim(),
        ));

    let visible: Vec<Line> = lines.into_iter().skip(scroll).collect();
    f.render_widget(Paragraph::new(visible).block(block), area);
}

fn draw_command_popup(f: &mut Frame, app: &App, area: Rect, input: &str, selected: usize) {
    let suggestions = app.command_suggestions(input);
    let height = (suggestions.len().min(12) as u16) + 5;
    let popup = centered_rect(64, height, area);
    f.render_widget(Clear, popup);

    let inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(2)])
        .split(popup);

    let input_block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(" Command (:) ", theme::title()));
    let input_line = Line::from(vec![
        Span::styled(
            " :",
            Style::default()
                .fg(theme::yellow())
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(input, Style::default().fg(theme::text())),
        Span::styled("█", Style::default().fg(theme::lavender())),
    ]);
    f.render_widget(Paragraph::new(input_line).block(input_block), inner[0]);

    let items: Vec<ListItem> = suggestions
        .iter()
        .take(12)
        .enumerate()
        .map(|(i, (cmd, desc))| {
            let style = if i == selected {
                theme::selected_row()
            } else {
                Style::default().fg(theme::text())
            };
            ListItem::new(Line::from(vec![
                Span::styled(format!(" {cmd:<18}"), style),
                Span::styled(
                    format!(" {desc}"),
                    if i == selected { style } else { theme::dim() },
                ),
            ]))
        })
        .collect();
    let list_block = Block::default()
        .borders(Borders::LEFT | Borders::RIGHT | Borders::BOTTOM)
        .border_style(theme::border_focused());
    f.render_widget(List::new(items).block(list_block), inner[1]);
}

fn draw_namespaces_popup(f: &mut Frame, app: &App, area: Rect, filter: &str, selected: usize) {
    let list = app.filtered_namespaces(filter);
    let height = (list.len().min(12) as u16) + 4;
    let popup = centered_rect(52, height, area);
    f.render_widget(Clear, popup);

    let title = if filter.is_empty() {
        " Switch Namespace (n) ".to_string()
    } else {
        format!(" Switch Namespace [/{filter}] ")
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(title, theme::title()));

    let items: Vec<ListItem> = list
        .iter()
        .take(12)
        .enumerate()
        .map(|(i, (ns, forbidden))| {
            let is_cur = app.active_namespace.as_deref() == Some(ns.as_str());
            let label = if ns == "*" {
                "* (all namespaces — admin)".to_string()
            } else if *forbidden {
                format!("{ns}  (403 forbidden)")
            } else if is_cur {
                format!("{ns}  • active")
            } else {
                ns.clone()
            };
            let style = if i == selected {
                theme::selected_row()
            } else if *forbidden {
                theme::dim()
            } else if is_cur {
                Style::default()
                    .fg(theme::green())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::text())
            };
            ListItem::new(Line::from(Span::styled(format!(" {label}"), style)))
        })
        .collect();

    f.render_widget(List::new(items).block(block), popup);
}

fn draw_contexts_popup(f: &mut Frame, app: &App, area: Rect, filter: &str, selected: usize) {
    let list = app.filtered_contexts(filter);
    let height = (list.len().min(10) as u16) + 4;
    let popup = centered_rect(68, height, area);
    f.render_widget(Clear, popup);

    let title = if filter.is_empty() {
        " Switch Context (:ctx) ".to_string()
    } else {
        format!(" Switch Context [/{filter}] ")
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(title, theme::title()));

    let items: Vec<ListItem> = list
        .iter()
        .take(10)
        .enumerate()
        .map(|(i, ctx)| {
            let ro = if ctx.readonly { "[RO]" } else { "[RW]" };
            let cur = if ctx.name == app.active_context {
                "•"
            } else {
                " "
            };
            let line_str = format!(
                " {cur} {:<16} {:<6} ns={:<14} {}",
                ctx.name, ro, ctx.namespace, ctx.api
            );
            let style = if i == selected {
                theme::selected_row()
            } else {
                Style::default().fg(theme::text())
            };
            ListItem::new(Line::from(Span::styled(line_str, style)))
        })
        .collect();
    f.render_widget(List::new(items).block(block), popup);
}

fn draw_sort_picker_popup(f: &mut Frame, app: &App, area: Rect, selected: usize) {
    let height = (app.columns.len() as u16) + 4;
    let popup = centered_rect(42, height, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(" Sort By Column (S) ", theme::title()));

    let items: Vec<ListItem> = app
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let arrow = match app.sort_col {
                Some((idx, true)) if idx == i => " ▼",
                Some((idx, false)) if idx == i => " ▲",
                _ => "",
            };
            let style = if i == selected {
                theme::selected_row()
            } else {
                Style::default().fg(theme::text())
            };
            ListItem::new(Line::from(Span::styled(
                format!(" {}{arrow}", col.header),
                style,
            )))
        })
        .collect();
    f.render_widget(List::new(items).block(block), popup);
}

fn draw_action_menu_popup(
    f: &mut Frame,
    app: &App,
    area: Rect,
    title: &str,
    entries: &[crate::app::actions::MenuEntry],
    selected: usize,
) {
    let height = (entries.len() as u16) + 4;
    let popup = centered_rect(72, height, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(format!(" {title} "), theme::title()));

    let items: Vec<ListItem> = entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let blocked = entry.mutating && app.is_readonly();
            let style = if i == selected {
                theme::selected_row()
            } else if blocked {
                theme::dim()
            } else {
                Style::default().fg(theme::text())
            };
            let ro_tag = if blocked { " [RO]" } else { "" };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!(" {:<32}", format!("{}{ro_tag}", entry.label)),
                    style,
                ),
                Span::styled(
                    format!(" {}", entry.description),
                    if i == selected { style } else { theme::dim() },
                ),
            ]))
        })
        .collect();
    f.render_widget(List::new(items).block(block), popup);
}

fn draw_confirm_popup(f: &mut Frame, area: Rect, title: &str, summary: &[String]) {
    let height = (summary.len() as u16).max(4) + 4;
    let popup = centered_rect(68, height, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::peach()))
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(theme::peach())
                .add_modifier(Modifier::BOLD),
        ))
        .title_bottom(Span::styled(
            " <y/Enter> confirm · <n/Esc> cancel ",
            theme::dim(),
        ));

    let lines: Vec<Line> = summary
        .iter()
        .map(|s| {
            Line::from(Span::styled(
                format!(" {s}"),
                Style::default().fg(theme::text()),
            ))
        })
        .collect();
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn draw_prompt_popup(f: &mut Frame, area: Rect, title: &str, prompt: &str, input: &str) {
    let popup = centered_rect(64, 7, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(format!(" {title} "), theme::title()))
        .title_bottom(Span::styled(
            " <Enter> submit · <Esc> cancel ",
            theme::dim(),
        ));

    let lines = vec![
        Line::from(Span::styled(format!(" {prompt}"), theme::dim())),
        Line::from(""),
        Line::from(vec![
            Span::styled(" > ", Style::default().fg(theme::yellow())),
            Span::styled(input.to_string(), Style::default().fg(theme::text())),
            Span::styled("█", Style::default().fg(theme::lavender())),
        ]),
    ];
    f.render_widget(Paragraph::new(lines).block(block), popup);
}

fn draw_skins_popup(f: &mut Frame, area: Rect, selected: usize) {
    let height = (theme::BUILTIN_NAMES.len().min(14) as u16) + 4;
    let popup = centered_rect(44, height, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::border_focused())
        .title(Span::styled(" Select Skin (K) ", theme::title()));

    let start = selected.saturating_sub(10);
    let items: Vec<ListItem> = theme::BUILTIN_NAMES
        .iter()
        .enumerate()
        .skip(start)
        .take(14)
        .map(|(i, name)| {
            let style = if i == selected {
                theme::selected_row()
            } else {
                Style::default().fg(theme::text())
            };
            ListItem::new(Line::from(Span::styled(format!(" {name}"), style)))
        })
        .collect();
    f.render_widget(List::new(items).block(block), popup);
}

fn draw_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let ns = app.active_namespace.as_deref().unwrap_or("*");
    let crumb = format!(
        " {} › {} › {} ",
        app.active_context, ns, app.active_resource.path
    );
    let pos = if app.rows.is_empty() {
        "0/0".to_string()
    } else {
        format!("{}/{}", app.cursor + 1, app.rows.len())
    };

    let (msg_text, msg_style) = if let Some((err, _)) = &app.poll_error {
        (
            format!("  {}", crate::redact::credentials(err)),
            Style::default()
                .fg(theme::red())
                .add_modifier(Modifier::BOLD),
        )
    } else if let Some((flash, is_err)) = &app.flash {
        let color = if *is_err {
            theme::red()
        } else {
            theme::green()
        };
        (
            format!("  {}", crate::redact::credentials(flash)),
            Style::default().fg(color),
        )
    } else {
        (String::new(), theme::dim())
    };

    let line = Line::from(vec![
        Span::styled(
            crumb,
            Style::default()
                .fg(theme::base())
                .bg(theme::mauve())
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(msg_text, msg_style),
        Span::raw("  "),
        Span::styled(format!("[{pos}]"), theme::dim()),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width.saturating_sub(2)).max(20);
    let h = height.min(area.height.saturating_sub(2)).max(4);
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    Rect::new(x, y, w, h)
}

/// Render a single frame of `app` to plain text using `TestBackend` (used by `--snapshot` and tests).
pub fn render_snapshot(app: &App, width: u16, height: u16) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = ratatui::Terminal::new(backend).expect("test terminal");
    terminal.draw(|f| draw(f, app)).expect("draw frame");
    let buf = terminal.backend().buffer();
    let mut out = String::new();
    for y in 0..buf.area.height {
        let mut line = String::new();
        for x in 0..buf.area.width {
            line.push_str(buf[(x, y)].symbol());
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}
