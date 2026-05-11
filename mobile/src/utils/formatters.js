export function formatNumber(value) {
  if (value === null || value === undefined) {
    return "0";
  }

  return Number(value).toLocaleString();
}

export function formatPercentage(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "N/A";
  }

  return `${Math.round(Number(value))}%`;
}

export function formatDate(value) {
  if (!value) {
    return "N/A";
  }

  return String(value);
}

export function formatTime(value) {
  if (!value) {
    return "N/A";
  }

  const rawValue = String(value);
  const timeMatch = rawValue.match(/T?(\d{2}:\d{2})/);
  return timeMatch ? timeMatch[1] : rawValue;
}
