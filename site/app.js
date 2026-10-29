import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6.16/+esm";

const DATA_URL = "data/dashboard.json";

const number = new Intl.NumberFormat("en-US");
const languageNames = new Intl.DisplayNames(["en"], { type: "language" });

const asDate = (iso) => new Date(`${iso}T00:00:00Z`);
const longDay = (iso) => asDate(iso).toLocaleDateString("en-GB", { day: "numeric", month: "long", timeZone: "UTC" });
const shortDay = (date) => date.toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
const percent = (x, digits = 0) => `${(x * 100).toFixed(digits)}%`;
const sum = (values) => values.reduce((a, b) => a + b, 0);
const mean = (values) => sum(values) / values.length;

function millions(n) {
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)} million`;
  return number.format(Math.round(n));
}

function compact(n) {
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)}M`;
  if (n >= 1e3) return `${Math.round(n / 1e3)}k`;
  return String(n);
}

function languageName(code) {
  if (code === "none") return "Not tagged";
  if (code === "other") return "Other";
  try {
    return languageNames.of(code) ?? code;
  } catch {
    return code;
  }
}

function colors() {
  const style = getComputedStyle(document.documentElement);
  const read = (name) => style.getPropertyValue(name).trim();
  return { ink: read("--ink"), soft: read("--ink-soft"), series1: read("--series-1"), series2: read("--series-2") };
}

function element(tag, attributes = {}, html = "") {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
  node.innerHTML = html;
  return node;
}

/* One section: the finding as its heading, a line of context, charts, and a margin note. */
function section({ title, lede, note, charts = [], body = null }) {
  const node = element("section", { class: "finding" });
  node.append(element("h2", {}, title));
  if (lede) node.append(element("p", { class: "lede" }, lede));
  for (const chart of charts) {
    const holder = element("figure", { class: "chart", "aria-label": chart.label });
    holder.style.margin = "0";
    if (chart.title) holder.append(element("figcaption", { class: "chart-title" }, chart.title));
    const canvas = element("div");
    holder.append(canvas);
    node.append(holder);
    chart.mount = canvas;
  }
  if (body) node.append(body);
  if (note) node.append(element("aside", { class: "note" }, note.map((p) => `<p>${p}</p>`).join("")));
  return node;
}

/* The shared look of every chart. */
function plotOptions(width, height, options) {
  return {
    width,
    height,
    marginLeft: 48,
    marginRight: 16,
    marginTop: 12,
    marginBottom: 30,
    style: { background: "transparent", fontSize: "12px", overflow: "visible" },
    ...options,
  };
}

function figures(data) {
  const whole = data.days.filter((d) => !d.partial);
  const recent = whole.slice(-(data.recent?.days ?? 7));
  return { whole, recent, latest: whole.at(-1) };
}

function activitySection(data, { whole, recent, latest }) {
  const averageActive = mean(recent.map((d) => d.daily_active));
  const averageNew = mean(recent.map((d) => d.new_accounts));
  const weekly = latest.weekly_complete
    ? ` In the week to ${longDay(latest.date)}, ${millions(latest.weekly_active)} wrote at least once.`
    : "";
  const charts = [
    {
      label: "Accounts that wrote something, by day",
      title: "Accounts that wrote something",
      draw: (width, c) =>
        Plot.plot(
          plotOptions(width, 220, {
            x: { type: "utc", tickFormat: shortDay, ticks: Math.min(whole.length, 8), label: null },
            y: { grid: true, zero: true, tickFormat: compact, label: null },
            marks: [
              Plot.ruleY([0], { stroke: c.soft, strokeOpacity: 0.4 }),
              Plot.lineY(whole, { x: (d) => asDate(d.date), y: "daily_active", stroke: c.series1, strokeWidth: 2 }),
              Plot.dot(whole, { x: (d) => asDate(d.date), y: "daily_active", fill: c.series1, r: 4, stroke: "var(--paper)", strokeWidth: 2 }),
              Plot.tip(
                whole,
                Plot.pointerX({
                  x: (d) => asDate(d.date),
                  y: "daily_active",
                  title: (d) => `${longDay(d.date)}\n${number.format(d.daily_active)} accounts`,
                }),
              ),
            ],
          }),
        ),
    },
    {
      label: "New accounts, by day",
      title: "New accounts",
      draw: (width, c) =>
        Plot.plot(
          plotOptions(width, 170, {
            x: { type: "band", tickFormat: (iso) => shortDay(asDate(iso)), label: null, padding: 0.35 },
            y: { grid: true, tickFormat: compact, label: null },
            marks: [
              Plot.barY(whole, { x: "date", y: "new_accounts", fill: c.series1, rx: 2, tip: { format: { x: (iso) => longDay(iso), y: (n) => number.format(n) } } }),
              Plot.ruleY([0], { stroke: c.soft, strokeOpacity: 0.4 }),
            ],
          }),
        ),
    },
  ];
  return {
    charts,
    node: section({
      title: `About ${millions(averageActive)} accounts write something each day, and ${number.format(Math.round(averageNew / 100) * 100)} are new.`,
      lede: `Averaged over the ${recent.length} days to ${longDay(latest.date)}.${weekly}`,
      note: [
        "Active means wrote something: a post, a like, a follow, a profile edit. The stream carries writes only, so accounts that only read never show up. These are active writers, not users.",
        "Bluesky's stream has no sign-up event. A new account is one whose first profile appears that day, with nothing seen from it earlier.",
      ],
      charts,
    }),
  };
}

function mixSection(data, { recent }) {
  const kinds = [
    ["Likes", "likes"],
    ["Posts", "posts"],
    ["Replies", "replies"],
    ["Reposts", "reposts"],
    ["Follows", "follows"],
    ["Deletes", "deletes"],
    ["Blocks", "blocks"],
  ];
  const rows = kinds
    .map(([name, key]) => ({ name, value: mean(recent.map((d) => d[key])) }))
    .sort((a, b) => b.value - a.value);
  const likes = rows.find((r) => r.name === "Likes").value;
  const writing = rows.filter((r) => r.name === "Posts" || r.name === "Replies").reduce((a, r) => a + r.value, 0);
  const ratio = likes / writing;
  const charts = [
    {
      label: "Average writes per day, by kind",
      draw: (width, c) =>
        Plot.plot(
          plotOptions(width, rows.length * 34 + 20, {
            marginLeft: 72,
            marginRight: 56,
            marginBottom: 8,
            x: { axis: null },
            y: { domain: rows.map((r) => r.name), label: null, tickSize: 0 },
            marks: [
              Plot.barX(rows, { y: "name", x: "value", fill: c.series1, rx: 2, insetTop: 4, insetBottom: 4 }),
              Plot.text(rows, { y: "name", x: "value", text: (r) => millions(r.value), textAnchor: "start", dx: 6, fill: c.ink }),
            ],
          }),
        ),
    },
  ];
  return {
    charts,
    node: section({
      title: `Likes outnumber posts and replies ${ratio.toFixed(0)} to 1.`,
      lede: `Writes of each kind on an average day, over the ${recent.length} days to ${longDay(recent.at(-1).date)}.`,
      note: [
        "A reply is a post with a parent, and is counted as a reply here, not a post.",
        "Deletes are of anything: an unlike or an unfollow is a delete too.",
      ],
      charts,
    }),
  };
}

function attentionSection(data, { whole, recent }) {
  const measured = whole.filter((d) => d.top_1pct_share != null);
  const share = mean(recent.filter((d) => d.top_1pct_share != null).map((d) => d.top_1pct_share));
  const gini = mean(recent.filter((d) => d.gini != null).map((d) => d.gini));
  const charts = [
    {
      label: "Share of engagement received by the top 1% of accounts, by day",
      title: "Share going to the top 1%",
      draw: (width, c) =>
        Plot.plot(
          plotOptions(width, 220, {
            x: { type: "utc", tickFormat: shortDay, ticks: Math.min(measured.length, 8), label: null },
            y: { grid: true, domain: [0, 1], tickFormat: (x) => percent(x), label: null },
            marks: [
              Plot.ruleY([0], { stroke: c.soft, strokeOpacity: 0.4 }),
              Plot.lineY(measured, { x: (d) => asDate(d.date), y: "top_1pct_share", stroke: c.series1, strokeWidth: 2 }),
              Plot.dot(measured, { x: (d) => asDate(d.date), y: "top_1pct_share", fill: c.series1, r: 4, stroke: "var(--paper)", strokeWidth: 2 }),
              Plot.tip(
                measured,
                Plot.pointerX({
                  x: (d) => asDate(d.date),
                  y: "top_1pct_share",
                  title: (d) => `${longDay(d.date)}\nTop 1%: ${percent(d.top_1pct_share, 1)}\nGini: ${d.gini.toFixed(2)}`,
                }),
              ),
            ],
          }),
        ),
    },
  ];
  return {
    charts,
    node: section({
      title: `${percent(share)} of all likes, reposts and replies go to the top 1% of accounts.`,
      lede: `Of the accounts that received any from others that day. The Gini coefficient averages ${gini.toFixed(2)}: 0 would mean every one of them got the same, 1 that a single account got it all.`,
      note: [
        "Liking, reposting or replying to your own posts doesn't count.",
        "Quote posts aren't counted: a quote lives in the post's content, which this project doesn't keep.",
      ],
      charts,
    }),
  };
}

function fadeSection(data) {
  const all = sum(data.post_age.map((r) => r.engagement));
  const byHour = data.post_age.filter((r) => r.age_hours >= 0 && r.age_hours < 48).map((r) => ({ ...r, share: r.engagement / all }));
  const shareUnder = (hours) => sum(data.post_age.filter((r) => r.age_hours >= 0 && r.age_hours < hours).map((r) => r.engagement)) / all;
  const older = sum(data.post_age.filter((r) => r.age_hours === 48).map((r) => r.engagement)) / all;
  let half = 1;
  while (half < 48 && shareUnder(half) < 0.5) half += 1;
  const charts = [
    {
      label: "Share of engagement by the age of the post, in hours",
      draw: (width, c) =>
        Plot.plot(
          plotOptions(width, 240, {
            x: { label: "Hours since the post was made", labelAnchor: "left", labelArrow: "none", ticks: [0, 6, 12, 18, 24, 30, 36, 42, 47] },
            y: { grid: true, tickFormat: (x) => percent(x), label: null },
            marks: [
              Plot.rectY(byHour, {
                x1: "age_hours",
                x2: (r) => r.age_hours + 1,
                y: "share",
                fill: (r) => (r.age_hours < half ? c.series1 : c.soft),
                fillOpacity: (r) => (r.age_hours < half ? 1 : 0.45),
                insetLeft: 1,
                insetRight: 1,
                rx: 1.5,
              }),
              Plot.ruleY([0], { stroke: c.soft, strokeOpacity: 0.4 }),
              Plot.tip(
                byHour,
                Plot.pointerX({
                  x: (r) => r.age_hours + 0.5,
                  y: "share",
                  title: (r) => `${r.age_hours} to ${r.age_hours + 1} hours old\n${percent(r.share, 1)} of engagement`,
                }),
              ),
            ],
          }),
        ),
    },
  ];
  const span = data.recent;
  return {
    charts,
    node: section({
      title: `${percent(shareUnder(1))} of likes, reposts and replies go to posts less than an hour old.`,
      lede: `Half goes to posts under ${half} ${half === 1 ? "hour" : "hours"} old, shown in blue, and ${percent(shareUnder(24))} to posts under a day. Over the ${span.days} days from ${longDay(span.start)} to ${longDay(span.end)}.`,
      note: [
        `A post's age comes from its key, which records when the author's app made it. ${percent(older, 1)} of engagement went to posts two days old or more, which the chart leaves out.`,
        "A like on a post from years ago counts here too: the post doesn't have to be one this project saw being made.",
      ],
      charts,
    }),
  };
}

function retentionSection(data) {
  const cohorts = new Map();
  for (const row of data.retention) {
    if (row.cohort_date <= data.collection_start) continue;
    if (!cohorts.has(row.cohort_date)) cohorts.set(row.cohort_date, { date: row.cohort_date, size: row.cohort_size });
    cohorts.get(row.cohort_date)[row.day_n] = row;
  }
  const rows = [...cohorts.values()].sort((a, b) => (a.date < b.date ? 1 : -1));
  const dayOne = data.retention.filter((r) => r.day_n === 1 && r.cohort_date > data.collection_start);
  const rate = sum(dayOne.map((r) => r.retained)) / sum(dayOne.map((r) => r.cohort_size));
  const cell = (row) => {
    if (!row) return `<td class="rate pending">not yet</td>`;
    return `<td class="rate" style="--alpha:${(0.08 + row.rate * 0.6).toFixed(2)}">${percent(row.rate, 1)}</td>`;
  };
  const table = element(
    "div",
    { class: "table-wrap" },
    `<table>
      <caption class="chart-title" style="text-align:left">Share of each day's new accounts that wrote something N days later</caption>
      <thead><tr><th scope="col">Signed up</th><th scope="col">New accounts</th><th scope="col">Day 1</th><th scope="col">Day 7</th><th scope="col">Day 30</th></tr></thead>
      <tbody>${rows
        .map((r) => `<tr><th scope="row" style="font-weight:400">${longDay(r.date)}</th><td>${number.format(r.size)}</td>${cell(r[1])}${cell(r[7])}${cell(r[30])}</tr>`)
        .join("")}</tbody>
    </table>`,
  );
  return {
    charts: [],
    node: section({
      title: Number.isFinite(rate)
        ? `${percent(rate)} of new accounts write something again the next day.`
        : "Retention starts once new accounts have had a day to come back.",
      lede: "Each row is the accounts that signed up that day. Day 7 and day 30 fill in as those days arrive.",
      note: [
        "Day N means exactly N days later, not within N days.",
        "Anything counts as writing again: a single like is enough.",
      ],
      body: table,
    }),
  };
}

function languagesSection(data) {
  const total = sum(data.languages.map((r) => r.posts));
  const named = data.languages.filter((r) => r.language !== "other").slice(0, 12);
  const otherPosts = total - sum(named.map((r) => r.posts));
  const rows = [...named.map((r) => ({ name: languageName(r.language), share: r.posts / total }))];
  if (otherPosts > 0) rows.push({ name: "Other", share: otherPosts / total });
  const english = data.languages.find((r) => r.language === "en");
  const untagged = data.languages.find((r) => r.language === "none");
  const charts = [
    {
      label: "Share of posts by first tagged language",
      draw: (width, c) =>
        Plot.plot(
          plotOptions(width, rows.length * 30 + 16, {
            marginLeft: 104,
            marginRight: 56,
            marginBottom: 8,
            x: { axis: null },
            y: { domain: rows.map((r) => r.name), label: null, tickSize: 0 },
            marks: [
              Plot.barX(rows, {
                y: "name",
                x: "share",
                fill: (r) => (r.name === "Other" || r.name === "Not tagged" ? c.soft : c.series1),
                fillOpacity: (r) => (r.name === "Other" || r.name === "Not tagged" ? 0.45 : 1),
                rx: 2,
                insetTop: 4,
                insetBottom: 4,
              }),
              Plot.text(rows, { y: "name", x: "share", text: (r) => percent(r.share, r.share < 0.1 ? 1 : 0), textAnchor: "start", dx: 6, fill: c.ink }),
            ],
          }),
        ),
    },
  ];
  return {
    charts,
    node: section({
      title:
        english && untagged
          ? `${percent(english.posts / total)} of posts are tagged English, and ${percent(untagged.posts / total)} carry no language tag at all.`
          : "What languages Bluesky posts in.",
      lede: `Posts and replies over the ${data.recent.days} days to ${longDay(data.recent.end)}, by the first language tagged.`,
      note: [
        "The tag is set by the author's app, not detected from the text.",
        `Languages with fewer than ${number.format(data.min_group)} posts a day are folded into “other”.`,
      ],
      charts,
    }),
  };
}

function pipelineSection(data, { latest }) {
  const writes = ["posts", "replies", "likes", "reposts", "follows", "blocks", "deletes"].reduce((a, k) => a + latest[k], 0);
  const updated = new Date(data.generated_at);
  const facts = [
    ["Writes on " + longDay(latest.date), millions(writes)],
    ["Collected since", longDay(data.collection_start)],
    ["Last updated", `${updated.toLocaleDateString("en-GB", { day: "numeric", month: "long", timeZone: "UTC" })}, ${updated.toISOString().slice(11, 16)} UTC`],
  ];
  if (data.deletes) {
    facts.push(["A deleted record stays visible for", `${data.deletes.average_minutes} minutes on average`]);
  }
  const list = element(
    "dl",
    { class: "facts" },
    facts.map(([term, value]) => `<div><dt>${term}</dt><dd>${value}</dd></div>`).join(""),
  );
  return {
    charts: [],
    node: section({
      title: `Every public write, collected around the clock since ${longDay(data.collection_start)}.`,
      lede: "A small server reads Bluesky's public stream, stores each event once, and rebuilds these totals every night.",
      note: data.deletes
        ? [
            `Deletes are honored: a deleted record is hidden from every number within a couple of hours (longest ${data.deletes.longest_minutes} minutes over the last ${data.deletes.runs} hourly runs), and its data is removed from storage on a schedule.`,
          ]
        : ["Deletes are honored: a deleted record is hidden from every number within a couple of hours, and its data is removed from storage on a schedule."],
      body: list,
    }),
  };
}

let sections = [];

function drawCharts() {
  const c = colors();
  for (const { charts } of sections) {
    for (const chart of charts) {
      const width = chart.mount.clientWidth || chart.mount.parentElement.clientWidth;
      chart.mount.replaceChildren(chart.draw(Math.max(280, width), c));
    }
  }
}

function render(data) {
  const report = document.getElementById("report");
  const parts = figures(data);
  if (!parts.latest) {
    document.getElementById("status").textContent = "There isn't a whole day of data yet. The first numbers appear the morning after the first full day.";
    return;
  }
  document.getElementById("headline").innerHTML =
    `On ${longDay(parts.latest.date)}, <span class="figure">${number.format(parts.latest.daily_active)}</span> accounts wrote something on Bluesky.`;
  document.getElementById("standfirst").textContent =
    `${number.format(parts.latest.new_accounts)} of them were new. Every number here comes from Bluesky's public stream of writes, and is rebuilt each night.`;
  document.getElementById("since").textContent = longDay(data.collection_start);
  document.getElementById("min-group").textContent = number.format(data.min_group);

  sections = [
    activitySection(data, parts),
    mixSection(data, parts),
    attentionSection(data, parts),
    fadeSection(data),
    retentionSection(data),
    languagesSection(data),
    pipelineSection(data, parts),
  ];
  report.replaceChildren(...sections.map((s) => s.node));
  drawCharts();
}

async function main() {
  let data;
  try {
    const response = await fetch(DATA_URL, { cache: "no-cache" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    data = await response.json();
  } catch (error) {
    document.getElementById("status").textContent =
      `The numbers couldn't be loaded (${error.message}). They're rebuilt every night; try again in a few minutes.`;
    return;
  }
  render(data);

  let lastWidth = document.getElementById("report").clientWidth;
  new ResizeObserver(() => {
    const width = document.getElementById("report").clientWidth;
    if (Math.abs(width - lastWidth) > 4) {
      lastWidth = width;
      drawCharts();
    }
  }).observe(document.getElementById("report"));
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", drawCharts);
}

main();
