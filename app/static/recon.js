// 发布对账渲染（主页面与 /reconciliation 独立页面共用）。
// 只消费 /api/reconciliation 返回的元信息：社区、发布时间、单次扣额、累计花费。
(function () {
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
    ));
  }

  function el(id) { return document.getElementById(id); }

  function setText(id, text, color) {
    const node = el(id);
    if (!node) return;
    node.textContent = text;
    if (color) node.style.color = color;
  }

  function render(d) {
    setText('rc-count', d.release_count + ' 笔');
    setText('rc-detail', d.ledger_charged_total.toFixed(2));
    setText('rc-legacy', d.legacy_spent == null ? '—' : d.legacy_spent.toFixed(2));
    setText('rc-ledger', d.budget_spent.toFixed(2));

    const verdict = el('rc-verdict');
    if (verdict) {
      if (!d.ledger_consistent) {
        verdict.textContent = '不一致';
        verdict.style.color = 'var(--red)';
      } else if (!d.all_verifiable) {
        verdict.textContent = '部分可核验';
        verdict.style.color = 'var(--amber)';
      } else {
        verdict.textContent = '一致 ✓';
        verdict.style.color = 'var(--green)';
      }
    }

    const body = el('rc-body');
    if (body) {
      body.innerHTML = d.entries.map(e => {
        const legacy = !e.verifiable;
        // 旧结果没有逐笔记录：扣额与累计花费留空，不显示 0（那会等于谎报未扣费）
        const charge = legacy ? '—（旧结果）' : e.epsilon.toFixed(2);
        const cum = (e.cumulative_spent == null) ? '—' : e.cumulative_spent.toFixed(2);
        const tag = legacy
          ? '<span class="tag warn">旧结果 · 无法逐笔核验</span>'
          : '<span class="tag ok">可核验</span>';
        return '<tr class="' + (legacy ? 'legacy' : '') + '">'
          + '<td>' + esc(e.rank) + '</td>'
          + '<td>' + esc(e.community) + '</td>'
          + '<td>' + esc(e.released_at) + '</td>'
          + '<td class="num">' + esc(charge) + '</td>'
          + '<td class="num">' + esc(cum) + '</td>'
          + '<td>' + tag + '</td></tr>';
      }).join('');
    }

    const empty = el('rc-empty');
    if (empty) empty.style.display = d.entries.length ? 'none' : 'block';

    const flags = el('rc-flags');
    if (flags) {
      flags.innerHTML = d.flags.map(f => {
        const kind = f.code === 'ledger_consistent' ? 'ok'
          : (f.code === 'legacy_unverifiable' ? 'warn' : 'bad');
        return '<div class="flag ' + kind + '">' + esc(f.message) + '</div>';
      }).join('');
    }
  }

  async function load() {
    const body = el('rc-body');
    const flags = el('rc-flags');
    try {
      const r = await fetch('/api/reconciliation');
      if (!r.ok) throw new Error('HTTP ' + r.status);
      render(await r.json());
    } catch (e) {
      if (body) body.innerHTML = '';
      if (flags) {
        flags.innerHTML = '<div class="flag bad">对账数据读取失败：'
          + esc(e.message) + '，请重试</div>';
      }
    }
  }

  window.DPRecon = { load: load, render: render, esc: esc };
})();
