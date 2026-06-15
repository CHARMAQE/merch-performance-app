import { ActivityIndicator, Pressable, ScrollView, Text, View } from "react-native";
import { DashboardFilter } from "../components/DashboardFilter";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatNumber } from "../utils/formatters";

export function DashboardScreen({
  supervisor,
  username,
  overview,
  isLoading,
  error,
  selectedMonth,
  selectedDay,
  startDate,
  endDate,
  onMonthChange,
  onDayChange,
  onStartDateChange,
  onEndDateChange,
  onRefresh,
  onLogout,
  onOpenProfile,
}) {
  const counts = overview?.tableCounts;
  const activity = overview?.storeActivity;
  const dailyReport = overview?.dailyReport;
  const displayName = username || supervisor?.fullName || supervisor?.username || "Supervisor";
  const profileInitials = getInitials(displayName);
  const plannedStores = Number(overview?.plannedVisits ?? activity?.plannedStores ?? 0);
  const visitsDone = Number(overview?.executedVisits ?? activity?.coveredStores ?? activity?.visits ?? 0);
  const deviationStores = Number(overview?.deviationVisits ?? activity?.deviationStores ?? 0);
  const visitedStores = visitsDone + deviationStores;
  const deviationRate = Math.round(Number(overview?.deviationRate ?? 0));
  const osaPercentage = getFirstDefined(
    overview?.osaPercentage,
    overview?.osaRate,
    overview?.osa
  );
  const sosPercentage = getFirstDefined(
    overview?.sosPercentage,
    overview?.sosRate,
    overview?.sos
  );
  const activeMerchandisers = Number(
    overview?.activeMerchandisers ?? dailyReport?.activeMerchandisers ?? counts?.employees ?? 0
  );
  const coverageRate = Math.round(
    Number(
      overview?.coverageRate ??
        (plannedStores > 0 ? (visitedStores / plannedStores) * 100 : 0)
    )
  );
  const coverageWidth = `${Math.min(100, Math.max(0, coverageRate))}%`;

  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>

      <View style={styles.dashboardHero}>
        <View style={styles.dashboardHeroProfileRow}>
          <Pressable style={styles.dashboardHeroProfileButton} onPress={onOpenProfile}>
            <View style={styles.heroProfileAvatar}>
              <Text style={styles.heroProfileAvatarText}>{profileInitials}</Text>
            </View>
            <View style={styles.heroProfileTextBlock}>
              <Text style={styles.heroProfileGreeting} numberOfLines={1}>
                Hello {displayName}
              </Text>
              <Text style={styles.heroProfileSubtitle}>Assigned perimeter</Text>
            </View>
          </Pressable>
          <Pressable style={styles.logoutButton} onPress={onLogout}>
            <Text style={styles.logoutButtonText}>Logout</Text>
          </Pressable>
        </View>

        <View style={styles.coverageBlock}>
          <View style={styles.coverageHeader}>
            <Text style={styles.coverageLabel}>Execution Coverage</Text>
            <Text style={styles.coverageValue}>{coverageRate}%</Text>
          </View>
          <View style={styles.coverageTrack}>
            <View style={[styles.coverageFill, { width: coverageWidth }]} />
          </View>
          <Text style={styles.coverageCountText}>
            {formatNumber(visitedStores)} covered from {formatNumber(plannedStores)} planned stores
          </Text>
        </View>
      </View>

      <DashboardFilter
        selectedMonth={selectedMonth}
        selectedDay={selectedDay}
        startDate={startDate}
        endDate={endDate}
        onMonthChange={onMonthChange}
        onDayChange={onDayChange}
        onStartDateChange={onStartDateChange}
        onEndDateChange={onEndDateChange}
        onApply={onRefresh}
      />

      {isLoading ? (
        <View style={styles.inlineState}>
          <ActivityIndicator color={colors.navy} />
          <Text style={styles.bodyText}>Loading dashboard...</Text>
        </View>
      ) : null}

      {error ? <Text style={styles.errorText}>{error}</Text> : null}

      <View style={styles.statsGrid}>
        <MetricCard label="Planned Stores" value={formatNumber(plannedStores)} detail="Planned perimeter" />
        <MetricCard label="Visits Done" value={formatNumber(visitsDone)} detail="Executed visits" status={rateStatus(coverageRate, "goodHigh")} />
        <MetricCard label="Deviation" value={formatNumber(deviationStores)} detail={`${deviationRate}% rate`} status={rateStatus(deviationRate, "badHigh")} />
        <MetricCard label="Active Merch" value={formatNumber(activeMerchandisers)} detail="In selected period" />
        <ProgressMetricCard label="OSA" value={osaPercentage} />
        <ProgressMetricCard label="SOS" value={sosPercentage} />
      </View>

      <Pressable style={styles.secondaryButton} onPress={onRefresh}>
        <Text style={styles.secondaryButtonText}>Refresh Data</Text>
      </Pressable>
    </ScrollView>
  );
}

function MetricCard({ detail, label, status, value }) {
  return (
    <View style={styles.metricCard}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text style={[styles.metricValue, status ? styles[`metricValue${capitalize(status)}`] : null]}>
        {value}
      </Text>
      {detail ? <Text style={styles.statDetail}>{detail}</Text> : null}
    </View>
  );
}

function ProgressMetricCard({ label, value }) {
  return (
    <View style={styles.progressMetricCard}>
      <View style={styles.progressMetricHeader}>
        <Text style={styles.progressMetricLabel}>{label}</Text>
        <Text style={styles.progressMetricValue}>{formatOptionalPercentage(value)}</Text>
      </View>
      <View style={styles.progressMetricTrack}>
        <View style={[styles.progressMetricFill, { width: progressWidth(value) }]} />
      </View>
    </View>
  );
}

function rateStatus(value, mode) {
  if (mode === "goodHigh") {
    if (value >= 85) return "success";
    if (value >= 65) return "warning";
    return "critical";
  }

  if (value <= 5) return "success";
  if (value <= 15) return "warning";
  return "critical";
}

function capitalize(value) {
  return value ? value.charAt(0).toUpperCase() + value.slice(1) : "";
}

function formatOptionalPercentage(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return `${Math.round(Number(value))}%`;
}

function getFirstDefined(...values) {
  return values.find((value) => value !== null && value !== undefined);
}

function getInitials(value) {
  return String(value)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part.charAt(0).toUpperCase())
    .join("") || "S";
}

function progressWidth(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "0%";
  }

  return `${Math.max(0, Math.min(100, Math.round(Number(value))))}%`;
}
