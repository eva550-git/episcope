(() => {
  const budget = document.getElementById('budget-control');
  const equity = document.getElementById('equity-control');
  const threshold = document.getElementById('threshold-control');
  const budgetLabel = document.getElementById('budget-value');
  const equityLabel = document.getElementById('equity-value');
  const thresholdLabel = document.getElementById('threshold-value');

  function update() {
    const scenario = {
      budgetFraction: Number(budget.value),
      equityFloor: Number(equity.value),
      threshold: Number(threshold.value),
    };
    budgetLabel.textContent = `${Math.round(scenario.budgetFraction * 100)}%`;
    equityLabel.textContent = `${Math.round(scenario.equityFloor * 100)}%`;
    thresholdLabel.textContent = `${scenario.threshold.toFixed(1)}σ`;
    window.EpiScopeDashboard.setScenario(scenario);
  }

  [budget, equity, threshold].forEach(control => control.addEventListener('input', update));
  update();
})();
