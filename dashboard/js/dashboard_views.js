(() => {
  const statusNode = document.getElementById('status');
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const fmt = d3.format(',');
  const fmt1 = d3.format(',.1f');
  const state = {
    disease: 'all',
    districtId: 'all',
    resource: 'iv_saline',
    threshold: 2,
    budgetFraction: 0.6,
    equityFloor: 0.25,
    dateRange: null,
  };
  let data;
  let boundaries;
  let districtById;
  let renderingTrend = false;

  function setStatus(message, error = false) {
    statusNode.textContent = message;
    statusNode.classList.toggle('error', error);
  }

  function inRange(date) {
    if (!state.dateRange) return true;
    const time = +date;
    return time >= +state.dateRange[0] && time <= +state.dateRange[1];
  }

  function diseaseMatches(disease) {
    return state.disease === 'all' || disease === state.disease;
  }

  function weeklyForView() {
    return data.district_weekly.filter(row =>
      diseaseMatches(row.disease)
      && (state.districtId === 'all' || row.district_id === state.districtId)
      && inRange(new Date(`${row.week_start}T00:00:00Z`))
    );
  }

  function activeCandidates() {
    if (state.dateRange && +state.dateRange[1] < +new Date(data.latest_week_date)) return [];
    return data.alert_candidates.filter(row =>
      row.z_score >= state.threshold
      && row.growth_pct >= 50
      && diseaseMatches(row.disease)
      && (state.districtId === 'all' || row.district_id === state.districtId)
    );
  }

  function displayCount(value) {
    return value === null || value === undefined ? 'Suppressed (1–4)' : fmt(Math.round(value));
  }

  function latestVisibleCases() {
    const latestRows = data.district_weekly.filter(row =>
      row.week_start === data.latest_week_date
      && diseaseMatches(row.disease)
      && (state.districtId === 'all' || row.district_id === state.districtId)
    );
    const visible = latestRows.reduce((total, row) => total + (row.cases ?? 0), 0);
    return { visible, suppressed: latestRows.filter(row => row.suppressed).length };
  }

  function renderSummary(activeSignals) {
    const counts = latestVisibleCases();
    const cards = [
      { label: 'Gold mart rows', value: fmt(data.row_count), note: 'District · disease · epidemiological week', icon: '▦' },
      {
        label: 'Visible latest-week cases',
        value: fmt(counts.visible),
        note: `Partial lower bound · ${fmt(counts.suppressed)} small cell${counts.suppressed === 1 ? '' : 's'} suppressed`,
        icon: '↗',
      },
      { label: 'Active warning signals', value: fmt(activeSignals.length), note: `Threshold ≥ ${fmt1(state.threshold)}σ · ≥50% growth`, icon: '!' },
      { label: 'Districts in selection', value: fmt(new Set(weeklyForView().map(row => row.district_id)).size), note: 'Based on the selected disease and time range', icon: '⌖' },
    ];
    const container = document.getElementById('summary');
    container.replaceChildren();
    for (const item of cards) {
      const card = document.createElement('article');
      card.className = 'card';
      const top = document.createElement('div');
      top.className = 'card-top';
      const label = document.createElement('div');
      label.className = 'card-label';
      label.textContent = item.label;
      const icon = document.createElement('span');
      icon.className = 'card-icon';
      icon.setAttribute('aria-hidden', 'true');
      icon.textContent = item.icon;
      top.append(label, icon);
      const value = document.createElement('div');
      value.className = 'card-value';
      value.textContent = item.value;
      const note = document.createElement('div');
      note.className = 'card-note';
      note.textContent = item.note;
      card.append(top, value, note);
      container.append(card);
    }
  }

  function renderMap(activeSignals) {
    const container = document.getElementById('map');
    container.replaceChildren();
    const values = new Map();
    const mapRows = data.district_weekly.filter(row =>
      diseaseMatches(row.disease) && inRange(new Date(`${row.week_start}T00:00:00Z`))
      && (state.districtId === 'all' || row.district_id === state.districtId)
      && (!state.dateRange ? row.week_start === data.latest_week_date : true)
    );
    for (const row of mapRows) {
      const entry = values.get(row.district_id) || { cases: 0, suppressed: false };
      if (row.cases === null) entry.suppressed ||= row.suppressed;
      else entry.cases += row.cases;
      values.set(row.district_id, entry);
    }
    for (const [id, entry] of values) {
      if (entry.cases > 0 && entry.cases < 5) {
        entry.suppressed = true;
        entry.cases = 0;
      }
    }
    const width = 650;
    const height = 410;
    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'img')
      .attr('aria-label', `District incidence map for ${state.disease}`);
    const projection = d3.geoMercator().fitSize([width, height], boundaries);
    const path = d3.geoPath(projection);
    const incidence = [];
    for (const [id, entry] of values) {
      const district = districtById.get(id);
      if (district?.population && entry.cases >= 5) {
        entry.incidence = entry.cases * 100_000 / district.population;
        incidence.push(entry.incidence);
      }
    }
    const color = d3.scaleSequential()
      .domain([0, d3.max(incidence) || 1])
      .interpolator(d3.interpolateYlOrRd);
    const features = svg.append('g').selectAll('path')
      .data(boundaries.features)
      .join('path')
      .attr('class', feature => `district-shape${feature.properties.adm2_pcode === state.districtId ? ' selected' : ''}`)
      .attr('d', path)
      .attr('fill', feature => {
        const entry = values.get(feature.properties.adm2_pcode);
        return entry?.incidence ? color(entry.incidence) : '#e6edef';
      })
      .on('click', (_event, feature) => selectDistrict(feature.properties.adm2_pcode))
      .append('title')
      .text(feature => {
        const code = feature.properties.adm2_pcode;
        const entry = values.get(code);
        const name = feature.properties.adm2_en || code;
        if (!entry) return `${name}: no visible records in this selection`;
        if (entry.suppressed && !entry.cases) return `${name}: one or more small cells suppressed`;
        return `${name}: ${displayCount(entry.cases)} visible cases · ${fmt1(entry.incidence || 0)} per 100,000`;
      });
    void features;

    const alerted = new Set(activeSignals.map(row => row.district_id));
    svg.append('g')
      .selectAll('circle')
      .data(boundaries.features.filter(feature => alerted.has(feature.properties.adm2_pcode)))
      .join('circle')
      .attr('class', 'alert-marker')
      .attr('r', 5.5)
      .attr('transform', feature => `translate(${path.centroid(feature)})`)
      .on('click', (_event, feature) => selectDistrict(feature.properties.adm2_pcode))
      .append('title')
      .text(feature => `${feature.properties.adm2_en}: active early-warning signal`);

    const legend = document.getElementById('map-legend');
    legend.replaceChildren();
    const valuesForLegend = d3.range(6).map(index => (d3.max(incidence) || 1) * index / 5);
    for (let index = 0; index < 5; index += 1) {
      const swatch = document.createElement('i');
      swatch.style.background = color(valuesForLegend[index + 1]);
      const interval = document.createElement('span');
      interval.textContent = `${fmt1(valuesForLegend[index])}–${fmt1(valuesForLegend[index + 1])}`;
      legend.append(swatch, interval);
    }
    const units = document.createElement('span');
    units.textContent = state.dateRange ? 'visible cases / 100k · selected period' : 'cases / 100k · latest week';
    legend.append(units);
  }

  function aggregateSeries(rows) {
    const groups = d3.group(rows, row => row.week_start);
    return [...groups.entries()]
      .map(([week, values]) => {
        const visible = values.filter(row => row.cases !== null);
        const hadSuppressed = values.some(row => row.suppressed);
        const total = d3.sum(visible, row => row.cases);
        return {
          date: new Date(`${week}T00:00:00Z`),
          cases: !visible.length && hadSuppressed ? null : (total > 0 && total < 5 ? null : total),
          suppressed: hadSuppressed || (total > 0 && total < 5),
        };
      })
      .sort((a, b) => a.date - b.date);
  }

  function trendRows() {
    const rows = weeklyForView();
    if (state.districtId !== 'all')     return aggregateSeries(rows);
    const national = rows.reduce((groups, row) => {
      const key = row.week_start;
      const entry = groups.get(key) || { date: new Date(`${key}T00:00:00Z`), cases: 0, suppressed: false };
      if (row.cases === null) entry.suppressed ||= row.suppressed;
      else entry.cases += row.cases;
      groups.set(key, entry);
      return groups;
    }, new Map());
    return [...national.values()]
      .map(row => {
        if (row.cases > 0 && row.cases < 5) return { ...row, cases: null, suppressed: true };
        if (!row.cases && row.suppressed) return { ...row, cases: null };
        return row;
      })
      .sort((a, b) => a.date - b.date);
  }

  function trendForecast() {
    let rows = state.districtId === 'all'
      ? data.forecast.filter(row => diseaseMatches(row.disease))
      : data.district_forecast.filter(row => row.district_id === state.districtId && diseaseMatches(row.disease));
    if (state.dateRange && +state.dateRange[1] < +new Date(data.latest_week_date)) return [];
    const grouped = d3.group(rows, row => row.week_start);
    return [...grouped.entries()].map(([week, entries]) => {
      const visible = entries.filter(row => row.forecast_cases !== null);
      return {
        date: new Date(`${week}T00:00:00Z`),
        cases: visible.length ? d3.sum(visible, row => row.forecast_cases) : null,
        lower: d3.sum(visible, row => row.lower_80 ?? 0),
        upper: d3.sum(visible, row => row.upper_80 ?? 0),
        suppressed: entries.some(row => row.suppressed),
      };
    }).sort((a, b) => a.date - b.date);
  }

  function renderTrend() {
    const container = document.getElementById('trend');
    container.replaceChildren();
    const actual = trendRows();
    const forecast = trendForecast();
    const visibleActual = actual.filter(row => row.cases !== null);
    if (!visibleActual.length) {
      container.innerHTML = '<p class="empty">No visible cases for this selection. Small cells are suppressed.</p>';
      return;
    }

    const width = 740;
    const height = 310;
    const margin = { top: 14, right: 18, bottom: 65, left: 54 };
    const chartBottom = 228;
    const fullDomainRows = data.district_weekly.filter(row =>
      diseaseMatches(row.disease)
      && (state.districtId === 'all' || row.district_id === state.districtId)
    );
    const fullDomain = d3.extent(fullDomainRows, row => new Date(`${row.week_start}T00:00:00Z`));
    if (!fullDomain[0] || !fullDomain[1]) {
      container.innerHTML = '<p class="empty">No time series is available for this selection.</p>';
      return;
    }
    const domain = state.dateRange
      ? [new Date(Math.max(+state.dateRange[0], +fullDomain[0])), new Date(Math.min(+state.dateRange[1], +fullDomain[1]))]
      : fullDomain;
    if (+domain[0] === +domain[1]) domain[1] = d3.utcDay.offset(domain[1], 7);
    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'img')
      .attr('aria-label', `Observed weekly cases, forecast and baseline alert limit for ${state.disease}`);
    const x = d3.scaleUtc().domain(domain).range([margin.left, width - margin.right]);
    const allValues = [...visibleActual.map(row => row.cases), ...forecast.map(row => row.upper ?? row.cases ?? 0)];
    const maximum = d3.max(allValues) || 1;
    const y = d3.scaleLinear().domain([0, maximum * 1.12]).nice().range([chartBottom, margin.top]);
    svg.append('g').attr('class', 'grid-line')
      .attr('transform', `translate(${margin.left},0)`)
      .call(d3.axisLeft(y).ticks(5).tickSize(-(width - margin.left - margin.right)).tickFormat(''));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(0,${chartBottom})`)
      .call(d3.axisBottom(x).ticks(5).tickFormat(d3.utcFormat('%b %y')));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(${margin.left},0)`)
      .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('~s')));

    if (visibleActual.length >= 8) {
      const baseline = visibleActual.slice(-8).map(row => row.cases);
      const mean = d3.mean(baseline);
      const std = d3.deviation(baseline) || 0;
      const thresholdValue = mean + state.threshold * std;
      svg.append('line')
        .attr('x1', margin.left).attr('x2', width - margin.right)
        .attr('y1', y(thresholdValue)).attr('y2', y(thresholdValue))
        .attr('stroke', '#ba3551').attr('stroke-width', 1.5).attr('stroke-dasharray', '5 4');
    }

    const area = d3.area()
      .defined(row => row.cases !== null && row.lower !== null && row.upper !== null)
      .x(row => x(row.date)).y0(row => y(row.lower)).y1(row => y(row.upper));
    if (forecast.length) {
      svg.append('path').datum(forecast).attr('fill', '#f4dfb8').attr('d', area);
      svg.append('path').datum(forecast)
        .attr('fill', 'none').attr('stroke', '#df8c27').attr('stroke-width', 2).attr('stroke-dasharray', '5 4')
        .attr('d', d3.line().defined(row => row.cases !== null).x(row => x(row.date)).y(row => y(row.cases)));
    }
    svg.append('path').datum(actual)
      .attr('fill', 'none').attr('stroke', '#147e78').attr('stroke-width', 2)
      .attr('d', d3.line().defined(row => row.cases !== null).x(row => x(row.date)).y(row => y(row.cases)));
    svg.append('g').selectAll('circle')
      .data(visibleActual)
      .join('circle')
      .attr('cx', row => x(row.date)).attr('cy', row => y(row.cases)).attr('r', 1.8)
      .attr('fill', '#147e78')
      .append('title').text(row => `${row.date.toISOString().slice(0, 10)}: ${fmt(row.cases)} visible cases`);

    const brushY = 265;
    const fullX = d3.scaleUtc().domain(fullDomain).range([margin.left, width - margin.right]);
    const brush = d3.brushX()
      .extent([[margin.left, brushY], [width - margin.right, brushY + 24]])
      .on('end', event => {
        if (renderingTrend || !event.selection) return;
        const selection = event.selection.map(fullX.invert);
        if (selection[1] - selection[0] < 7 * 24 * 60 * 60 * 1000) {
          state.dateRange = null;
        } else {
          state.dateRange = selection;
        }
        const start = state.dateRange?.[0].toISOString().slice(0, 10);
        const end = state.dateRange?.[1].toISOString().slice(0, 10);
        document.getElementById('time-range-label').textContent = start ? `${start} – ${end}` : 'All available weeks';
        renderLinkedViews();
        renderTrend();
      });
    const brushGroup = svg.append('g').attr('class', 'brush').call(brush);
    if (state.dateRange) {
      renderingTrend = true;
      brushGroup.call(brush.move, state.dateRange.map(fullX));
      renderingTrend = false;
    } else {
      renderingTrend = true;
      brushGroup.call(brush.move, null);
      renderingTrend = false;
    }
  }

  function renderSeasonality() {
    const container = document.getElementById('seasonality');
    container.replaceChildren();
    const groups = new Map();
    for (const row of weeklyForView()) {
      const date = new Date(`${row.week_start}T00:00:00Z`);
      const thursday = new Date(date);
      thursday.setUTCDate(date.getUTCDate() + 3 - ((date.getUTCDay() + 6) % 7));
      const year = thursday.getUTCFullYear();
      const januaryFourth = new Date(Date.UTC(year, 0, 4));
      januaryFourth.setUTCDate(januaryFourth.getUTCDate() + 3 - ((januaryFourth.getUTCDay() + 6) % 7));
      const week = 1 + Math.round((thursday - januaryFourth) / (7 * 24 * 60 * 60 * 1000));
      const key = `${year}|${week}`;
      const entry = groups.get(key) || { year, week, cases: 0, suppressed: false, records: 0 };
      if (row.cases === null) entry.suppressed ||= row.suppressed;
      else entry.cases += row.cases;
      entry.records += 1;
      groups.set(key, entry);
    }
    const yearRange = d3.extent([...groups.values()], row => row.year);
    if (!yearRange[0]) {
      container.innerHTML = '<p class="empty">No seasonal data for this selection.</p>';
      return;
    }
    const years = d3.range(yearRange[0], yearRange[1] + 1);
    const cells = [];
    for (const year of years) {
      for (let week = 1; week <= 53; week += 1) {
        const row = groups.get(`${year}|${week}`);
        const total = row?.cases ?? 0;
        const suppressed = row?.suppressed || (total > 0 && total < 5);
        cells.push({
          year,
          week,
          cases: suppressed && total < 5 ? null : total,
          suppressed,
          hasData: Boolean(row),
        });
      }
    }
    const width = 740;
    const height = Math.max(125, 34 + years.length * 26);
    const margin = { top: 10, right: 14, bottom: 28, left: 48 };
    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'img')
      .attr('aria-label', 'Epidemiological week by year disease seasonality heatmap');
    const x = d3.scaleBand().domain(d3.range(1, 54)).range([margin.left, width - margin.right]).padding(.12);
    const y = d3.scaleBand().domain(years).range([margin.top, height - margin.bottom]).padding(.13);
    const max = d3.max(cells, row => row.cases ?? 0) || 1;
    const color = d3.scaleSequential([0, max], d3.interpolateViridis);
    svg.append('g').attr('class', 'axis').attr('transform', `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).tickValues([1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53]).tickFormat(week => `W${week}`));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(${margin.left},0)`)
      .call(d3.axisLeft(y));
    svg.selectAll('rect').data(cells).join('rect')
      .attr('x', row => x(row.week)).attr('y', row => y(row.year))
      .attr('width', x.bandwidth()).attr('height', y.bandwidth()).attr('rx', 2)
      .attr('fill', row => !row.hasData || row.cases === null ? '#e6edef' : color(row.cases))
      .append('title')
      .text(row => `Epi-year ${row.year} · week ${row.week}: ${
        !row.hasData ? 'no source record'
          : row.suppressed && row.cases === null ? 'small cells suppressed'
            : `${fmt(row.cases)} visible cases`
      }`);
  }

  function renderPreparednessCalendar() {
    const container = document.getElementById('preparedness-calendar');
    container.replaceChildren();
    const selectedDistrict = state.districtId === 'all' ? null : districtById.get(state.districtId);
    const rows = (data.preparedness_calendar || []).filter(row =>
      diseaseMatches(row.disease)
      && (!selectedDistrict || row.division === selectedDistrict.division)
    );
    if (!rows.length) {
      container.innerHTML = '<p class="empty">No next-year seasonal decomposition is available for this selection.</p>';
      return;
    }

    const rowKeys = [...new Set(rows.map(row => `${row.division}|${row.disease}`))].sort();
    const cells = new Map(rows.map(row => [`${row.division}|${row.disease}|${row.month}`, row]));
    const width = 740;
    const height = Math.max(130, 48 + rowKeys.length * 26);
    const margin = { top: 12, right: 14, bottom: 30, left: 145 };
    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'img')
      .attr('aria-label', 'Next-year monthly disease preparedness calendar by division');
    const x = d3.scaleBand().domain(d3.range(1, 13))
      .range([margin.left, width - margin.right]).padding(.12);
    const y = d3.scaleBand().domain(rowKeys)
      .range([margin.top, height - margin.bottom]).padding(.13);
    const visible = rows.filter(row => row.expected_cases !== null);
    const maximum = d3.max(visible, row => row.expected_cases) || 1;
    const color = d3.scaleSequential([0, maximum], d3.interpolateViridis);
    const monthFormat = d3.utcFormat('%b');
    svg.append('g').attr('class', 'axis').attr('transform', `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).tickFormat(month => monthFormat(new Date(Date.UTC(2025, month - 1, 1)))));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(${margin.left},0)`)
      .call(d3.axisLeft(y).tickFormat(key => key.replace('|', ' · ')));
    const allCells = rowKeys.flatMap(key =>
      d3.range(1, 13).map(month => ({ key, month, row: cells.get(`${key}|${month}`) }))
    );
    svg.selectAll('rect').data(allCells).join('rect')
      .attr('x', cell => x(cell.month)).attr('y', cell => y(cell.key))
      .attr('width', x.bandwidth()).attr('height', y.bandwidth()).attr('rx', 2)
      .attr('fill', cell => !cell.row || cell.row.expected_cases === null
        ? '#e6edef'
        : color(cell.row.expected_cases))
      .append('title')
      .text(cell => {
        if (!cell.row) return `${cell.key.replace('|', ' · ')} · month ${cell.month}: no decomposition`;
        const count = cell.row.expected_cases === null
          ? 'small forecast suppressed'
          : `${fmt(cell.row.expected_cases)} expected cases`;
        return `${cell.key.replace('|', ' · ')} · ${monthFormat(new Date(Date.UTC(cell.row.calendar_year, cell.month - 1, 1)))} ${cell.row.calendar_year}: ${count} · ${cell.row.action}`;
      });
  }

  function renderDataQualityReport() {
    const container = document.getElementById('dq-report');
    container.replaceChildren();
    const report = data.data_quality_report;
    if (!report?.available) {
      const message = document.createElement('p');
      message.className = 'empty';
      message.textContent = report?.message || 'DQ report is unavailable in this serving export.';
      container.append(message);
      return;
    }

    const summary = document.createElement('div');
    summary.className = 'dq-summary';
    for (const [label, value] of [
      ['Load time (UTC)', report.generated_at_utc],
      ['Input rows', fmt(report.rows_in)],
      ['Clean rows', fmt(report.rows_out)],
      ['Quarantined', fmt(report.rows_quarantined)],
    ]) {
      const item = document.createElement('span');
      item.textContent = `${label}: `;
      const strong = document.createElement('strong');
      strong.textContent = value ?? '—';
      item.append(strong);
      summary.append(item);
    }
    renderTable(
      container,
      [
        { key: 'rule_code', label: 'Rule' },
        { key: 'description', label: 'Validation' },
        { key: 'enabled', label: 'Enabled', format: value => value ? 'Yes' : 'No' },
        { key: 'pass_count', label: 'Passed', numeric: true, format: value => value === null ? '—' : fmt(value) },
        { key: 'fail_count', label: 'Failed', numeric: true, format: value => value === null ? '—' : fmt(value) },
      ],
      report.rules || [],
      'The latest DQ report contains no validation rules.'
    );
    container.prepend(summary);
  }

  function renderTable(node, columns, rows, emptyMessage, rowClick) {
    node.replaceChildren();
    if (!rows.length) {
      const empty = document.createElement('p');
      empty.className = 'empty';
      empty.textContent = emptyMessage;
      node.append(empty);
      return;
    }
    const table = document.createElement('table');
    const header = table.createTHead().insertRow();
    for (const column of columns) {
      const cell = document.createElement('th');
      cell.textContent = column.label;
      if (column.numeric) cell.className = 'numeric';
      header.append(cell);
    }
    const body = table.createTBody();
    rows.forEach(row => {
      const tableRow = body.insertRow();
      if (row.district_id === state.districtId) tableRow.className = 'selected';
      if (rowClick) {
        tableRow.tabIndex = 0;
        tableRow.addEventListener('click', () => rowClick(row));
        tableRow.addEventListener('keydown', event => {
          if (event.key === 'Enter' || event.key === ' ') rowClick(row);
        });
      }
      for (const column of columns) {
        const cell = tableRow.insertCell();
        const value = row[column.key];
        if (column.badge) {
          const badge = document.createElement('span');
          badge.className = 'badge';
          badge.textContent = column.format ? column.format(value) : String(value ?? 'Alert');
          cell.append(badge);
        } else {
          cell.textContent = column.format ? column.format(value, row) : (value ?? '—');
        }
        if (column.numeric) cell.className = 'numeric';
      }
    });
    node.append(table);
  }

  function renderAlerts(activeSignals) {
    const rows = [...activeSignals].sort((a, b) => b.z_score - a.z_score);
    const displayedRows = rows.slice(0, 30);
    document.getElementById('alerts-subtitle').textContent = rows.length > displayedRows.length
      ? `Showing top ${fmt(displayedRows.length)} of ${fmt(rows.length)} qualifying signals`
      : `${fmt(rows.length)} qualifying signals at ≥ ${fmt1(state.threshold)}σ and ≥50% growth`;
    renderTable(
      document.getElementById('alerts'),
      [
        { key: 'z_score', label: 'Rank / σ', numeric: true, format: (_value, row) => `${rows.indexOf(row) + 1} · ${fmt1(row.z_score)}σ` },
        { key: 'district', label: 'District' },
        { key: 'disease', label: 'Disease' },
        { key: 'recent_cases', label: '4-week cases', numeric: true, format: displayCount },
        { key: 'growth_pct', label: 'Increase', numeric: true, format: value => `${fmt1(value)}%` },
        { key: 'status', label: 'Signal', badge: true, format: () => 'Alert' },
      ],
      displayedRows,
      'No districts meet the selected early-warning threshold.',
      selectAlert
    );
  }

  function selectedScenario() {
    return data.allocation_scenarios.reduce((best, scenario) => {
      const score = Math.abs(scenario.budget_fraction - state.budgetFraction)
        + Math.abs(scenario.equity_floor - state.equityFloor);
      return score < best.score ? { scenario, score } : best;
    }, { scenario: data.allocation_scenarios[0], score: Infinity }).scenario;
  }

  function allocationRows() {
    const scenario = selectedScenario();
    const applicableResources = state.disease === 'all'
      ? null
      : ({
        dengue: new Set(['beds', 'iv_saline', 'ns1_kits']),
        diarrhoea: new Set(['beds', 'iv_saline', 'ors']),
        ili: new Set(['beds', 'iv_saline']),
        malaria: new Set(['beds', 'iv_saline', 'antimalarials']),
      }[state.disease] || null);
    return scenario.allocations.filter(row =>
      row.resource === state.resource
      && (!applicableResources || applicableResources.has(row.resource))
      && (state.districtId === 'all' || row.district_id === state.districtId)
    ).map(row => ({
      ...row,
      district: districtById.get(row.district_id)?.district || row.district_id,
      division: districtById.get(row.district_id)?.division || '',
    })).sort((a, b) => (b.demand ?? -1) - (a.demand ?? -1));
  }

  function bars(containerId, rows) {
    const container = document.getElementById(containerId);
    container.replaceChildren();
    if (!rows.length) {
      container.innerHTML = '<p class="empty">No allocation data for this selection.</p>';
      return;
    }
    const width = 730;
    const height = Math.max(190, 35 + rows.length * 24);
    const margin = { top: 7, right: 14, bottom: 26, left: 105 };
    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'img')
      .attr('aria-label', `${data.resource_labels[state.resource]} demand, stock and allocation`);
    const maximum = d3.max(rows, row => Math.max(row.demand ?? 0, row.current_stock ?? 0, row.allocated ?? 0, row.baseline_allocation ?? 0)) || 1;
    const x = d3.scaleLinear().domain([0, maximum * 1.1]).nice().range([margin.left, width - margin.right]);
    const y = d3.scaleBand().domain(rows.map(row => row.district_id)).range([margin.top, height - margin.bottom]).padding(.18);
    svg.append('g').attr('class', 'axis').attr('transform', `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('~s')));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(${margin.left},0)`)
      .call(d3.axisLeft(y).tickFormat(id => districtById.get(id)?.district || id));
    const series = [
      { key: 'demand', color: '#7593a3', offset: 0 },
      { key: 'current_stock', color: '#a8cbbb', offset: 4 },
      { key: 'allocated', color: '#23867b', offset: 8 },
      { key: 'baseline_allocation', color: '#d59d40', offset: 12 },
    ];
    for (const item of series) {
      svg.selectAll(`rect.${item.key}`).data(rows).join('rect')
        .attr('class', item.key)
        .attr('x', margin.left)
        .attr('y', row => y(row.district_id) + item.offset)
        .attr('width', row => x(row[item.key] ?? 0) - margin.left)
        .attr('height', 3.5)
        .attr('rx', 1.5)
        .attr('fill', item.color)
        .append('title')
        .text(row => `${row.district}: ${item.key.replace('_', ' ')} ${displayCount(row[item.key])}`);
    }
  }

  function renderAllocationCurve() {
    const container = document.getElementById('allocation-curve');
    container.replaceChildren();
    const width = 440;
    const height = 225;
    const margin = { top: 14, right: 44, bottom: 34, left: 40 };
    const svg = d3.select(container).append('svg').attr('viewBox', `0 0 ${width} ${height}`)
      .attr('role', 'img').attr('aria-label', 'Efficiency and equity versus national budget');
    const x = d3.scaleLinear().domain([0, 1]).range([margin.left, width - margin.right]);
    const y = d3.scaleLinear().domain([0, 100]).range([height - margin.bottom, margin.top]);
    svg.append('g').attr('class', 'axis').attr('transform', `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).ticks(5).tickFormat(value => `${value * 100}%`));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(${margin.left},0)`)
      .call(d3.axisLeft(y).ticks(5).tickFormat(value => `${value}%`));
    svg.append('g').attr('class', 'axis').attr('transform', `translate(${width - margin.right},0)`)
      .call(d3.axisRight(y).ticks(5).tickFormat(value => `${value}%`));
    const curves = [
      { key: 'optimized_fill_pct', color: '#23867b', axis: y, label: 'Optimized efficiency' },
      { key: 'baseline_fill_pct', color: '#d59d40', axis: y, label: 'Population baseline efficiency' },
      { key: 'optimized_equity_pct', color: '#315d9a', axis: y, label: 'Optimized equity' },
      { key: 'baseline_equity_pct', color: '#8799ad', axis: y, label: 'Baseline equity' },
    ];
    for (const curve of curves) {
      svg.append('path').datum(data.allocation_curve)
        .attr('fill', 'none').attr('stroke', curve.color).attr('stroke-width', 1.8)
        .attr('d', d3.line().x(row => x(row.budget_fraction)).y(row => curve.axis(row[curve.key])));
    }
    const selected = data.allocation_curve.find(row => row.budget_fraction === state.budgetFraction);
    if (selected) {
      svg.append('line').attr('x1', x(selected.budget_fraction)).attr('x2', x(selected.budget_fraction))
        .attr('y1', margin.top).attr('y2', height - margin.bottom)
        .attr('stroke', '#597586').attr('stroke-dasharray', '3 3');
    }
    const legend = document.getElementById('allocation-curve');
    void legend;
  }

  function renderAllocations() {
    const rows = allocationRows();
    bars('allocation-chart', rows.slice(0, state.districtId === 'all' ? 12 : 1));
    renderTable(
      document.getElementById('allocations'),
      [
        { key: 'district', label: 'District' },
        { key: 'division', label: 'Division' },
        { key: 'demand', label: 'Forecast demand', numeric: true, format: displayCount },
        { key: 'current_stock', label: 'On-hand stock', numeric: true, format: displayCount },
        { key: 'allocated', label: 'Optimized dispatch', numeric: true, format: displayCount },
        { key: 'baseline_allocation', label: 'Population baseline', numeric: true, format: displayCount },
        { key: 'unmet', label: 'Unmet after dispatch', numeric: true, format: displayCount },
      ],
      rows,
      'No recommended allocations for this selection.',
      row => selectDistrict(row.district_id)
    );
    renderAllocationCurve();
    document.getElementById('resource-note').textContent =
      `${data.assumptions.allocation_method} Unit costs and stock are synthetic planning assumptions; validate against a real facility stock feed.`;
  }

  function updateFreshness() {
    document.getElementById('header-freshness').textContent =
      `Epi week: ${data.latest_week_date} · stock: ${data.data_freshness_date}`;
    document.querySelectorAll('.view-freshness').forEach(node => {
      if (node.dataset.freshness === 'quality') {
        const generated = data.data_quality_report?.generated_at_utc;
        node.textContent = generated ? `DQ load ${generated.slice(0, 10)}` : 'DQ load date unavailable';
      } else if (node.dataset.freshness === 'stock') {
        node.textContent = `Stock ${data.data_freshness_date}`;
      } else {
        node.textContent = `Week ending ${data.latest_week_date}`;
      }
    });
  }

  function renderLinkedViews() {
    const activeSignals = activeCandidates();
    renderSummary(activeSignals);
    renderMap(activeSignals);
    renderSeasonality();
    renderPreparednessCalendar();
    renderDataQualityReport();
    renderAlerts(activeSignals);
    renderAllocations();
  }

  function selectDistrict(districtId) {
    state.districtId = state.districtId === districtId ? 'all' : districtId;
    document.getElementById('district-select').value = state.districtId;
    renderLinkedViews();
    renderTrend();
  }

  function selectAlert(row) {
    state.districtId = row.district_id;
    state.disease = row.disease;
    document.getElementById('district-select').value = row.district_id;
    document.getElementById('disease-select').value = row.disease;
    renderLinkedViews();
    renderTrend();
  }

  function wireControls() {
    const diseaseSelect = document.getElementById('disease-select');
    diseaseSelect.replaceChildren();
    [['all', 'All diseases'], ...data.diseases.map(disease => [disease, disease[0].toUpperCase() + disease.slice(1)])]
      .forEach(([value, label]) => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = label;
        diseaseSelect.append(option);
      });
    diseaseSelect.disabled = false;
    diseaseSelect.addEventListener('change', event => {
      state.disease = event.currentTarget.value;
      renderLinkedViews();
      renderTrend();
    });

    const districtSelect = document.getElementById('district-select');
    districtSelect.replaceChildren();
    const allOption = document.createElement('option');
    allOption.value = 'all';
    allOption.textContent = 'All districts';
    districtSelect.append(allOption);
    [...districtById.values()].sort((a, b) => a.district.localeCompare(b.district)).forEach(row => {
      const option = document.createElement('option');
      option.value = row.district_id;
      option.textContent = `${row.district} · ${row.division}`;
      districtSelect.append(option);
    });
    districtSelect.disabled = false;
    districtSelect.addEventListener('change', event => {
      state.districtId = event.currentTarget.value;
      renderLinkedViews();
      renderTrend();
    });

    const resourceSelect = document.getElementById('resource-select');
    resourceSelect.replaceChildren();
    Object.entries(data.resource_labels).forEach(([resource, label]) => {
      const option = document.createElement('option');
      option.value = resource;
      option.textContent = label;
      resourceSelect.append(option);
    });
    resourceSelect.disabled = false;
    resourceSelect.value = state.resource;
    resourceSelect.addEventListener('change', event => {
      state.resource = event.currentTarget.value;
      renderAllocations();
    });

    document.getElementById('reset-range').addEventListener('click', () => {
      state.dateRange = null;
      document.getElementById('time-range-label').textContent = 'All available weeks';
      renderLinkedViews();
      renderTrend();
    });

    const budget = document.getElementById('budget-control');
    const equity = document.getElementById('equity-control');
    const threshold = document.getElementById('threshold-control');
    const budgetLabel = document.getElementById('budget-value');
    const equityLabel = document.getElementById('equity-value');
    const thresholdLabel = document.getElementById('threshold-value');
    [budgetLabel, equityLabel, thresholdLabel].forEach((node, index) => {
      node.textContent = index === 0 ? '60%' : index === 1 ? '25%' : '2.0σ';
    });
    renderLinkedViews();
    renderTrend();
  }

  async function load() {
    try {
      if (!window.d3) throw new Error('D3.js did not load; check the internet connection and refresh.');
      const [summaryResponse, boundariesResponse] = await Promise.all([
        fetch('../serving/dashboard_summary.json', { cache: 'no-store' }),
        fetch('../serving/district_boundaries.geojson', { cache: 'no-store' }),
      ]);
      if (!summaryResponse.ok) throw new Error(`Dashboard data request failed (${summaryResponse.status}). Run the serving export first.`);
      if (!boundariesResponse.ok) throw new Error(`Map boundary request failed (${boundariesResponse.status}). Run the serving export first.`);
      [data, boundaries] = await Promise.all([summaryResponse.json(), boundariesResponse.json()]);
      data.district_weekly = data.district_weekly.map(([districtIndex, diseaseIndex, weekIndex, cases]) => ({
        district_id: data.districts[districtIndex].district_id,
        disease: data.diseases[diseaseIndex],
        week_start: data.weeks[weekIndex],
        cases,
        suppressed: cases === null,
      }));
      districtById = new Map(data.districts.map(row => [row.district_id, row]));
      if (!data.district_weekly?.length || !data.allocation_scenarios?.length) {
        throw new Error('Exported data is missing district history or optimized allocation scenarios.');
      }
      wireControls();
      updateFreshness();
      setStatus(
        `Ready · week ending ${data.latest_week_date} · ${districtById.size} districts · `
        + `${fmt(data.assumptions.small_cells_suppressed)} district-week small cells suppressed.`
      );
    } catch (error) {
      setStatus(error.message || String(error), true);
    }
  }

  window.EpiScopeDashboard = {
    setScenario(next) {
      Object.assign(state, next);
      if (!data) return;
      renderLinkedViews();
      renderTrend();
    },
  };

  load();
})();
