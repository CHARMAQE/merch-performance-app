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
}) {
  const counts = overview?.tableCounts;
  const activity = overview?.storeActivity;
  const dailyReport = overview?.dailyReport;
  const assignedStores = Number(activity?.assignedStores ?? counts?.stores ?? 0);
  const visitedStores = Number(activity?.visitedStores ?? 0);
  const plannedStores = Number(overview?.plannedVisits ?? activity?.plannedStores ?? visitedStores);
  const coveredStores = Number(overview?.executedVisits ?? activity?.coveredStores ?? visitedStores);
  const nonVisitedStores = Number(overview?.nonVisitedVisits ?? activity?.notVisitedStores ?? 0);
  const deviationStores = Number(overview?.deviationVisits ?? activity?.deviationStores ?? 0);
  const rejectedStores = Number(overview?.rejectedVisits ?? 0);
  const problematicVisits = Number(overview?.problematicVisits ?? 0);
  const taskCompletionRate = Math.round(Number(overview?.taskCompletionRate ?? 0));
  const nonVisitedRate = Math.round(Number(overview?.nonVisitedRate ?? 0));
  const deviationRate = Math.round(Number(overview?.deviationRate ?? 0));
  const rejectionRate = Math.round(Number(overview?.rejectionRate ?? 0));
  const activeMerchandisers = Number(
    overview?.activeMerchandisers ?? dailyReport?.activeMerchandisers ?? counts?.employees ?? 0
  );
  const coverageRate = Math.round(
    Number(
      overview?.coverageRate ??
        (plannedStores > 0 ? (coveredStores / plannedStores) * 100 : 0)
    )
  );
  const coverageWidth = `${Math.min(100, Math.max(0, coverageRate))}%`;

  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>
      <View style={styles.dashboardHero}>
        <View style={styles.headerRow}>
          <View style={styles.headerTitleBlock}>
            <Text style={styles.heroEyebrow}>UNILEVER FIELD SUPERVISION</Text>
            <Text style={styles.dashboardTitle}>Hello {username}</Text>
            <Text style={styles.dashboardSubtitle}>
              Store execution for your assigned perimeter
            </Text>
          </View>
          <Pressable style={styles.logoutButton} onPress={onLogout}>
            <Text style={styles.logoutButtonText}>Logout</Text>
          </Pressable>
        </View>

        <View style={styles.coverageBlock}>
          <View style={styles.coverageHeader}>
            <Text style={styles.coverageLabel}>Coverage</Text>
            <Text style={styles.coverageValue}>{coverageRate}%</Text>
          </View>
          <View style={styles.coverageTrack}>
            <View style={[styles.coverageFill, { width: coverageWidth }]} />
          </View>
          <Text style={styles.coverageMeta}>
            {formatNumber(coveredStores)} covered from {formatNumber(plannedStores)} planned stores
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
        <MetricCard label="Planned Visits" value={formatNumber(plannedStores)} detail={`${formatNumber(assignedStores)} stores`} />
        <MetricCard label="Executed Visits" value={formatNumber(coveredStores)} detail="Completed visits" status={rateStatus(coverageRate, "goodHigh")} />
        <MetricCard label="Coverage" value={`${coverageRate}%`} detail="Executed / planned" status={rateStatus(coverageRate, "goodHigh")} />
        <MetricCard label="Non Visited" value={formatNumber(nonVisitedStores)} detail={`${nonVisitedRate}% rate`} status={rateStatus(nonVisitedRate, "badHigh")} />
        <MetricCard label="Deviations" value={formatNumber(deviationStores)} detail={`${deviationRate}% rate`} status={rateStatus(deviationRate, "badHigh")} />
        <MetricCard label="Rejected" value={formatNumber(rejectedStores)} detail={`${rejectionRate}% rate`} status={rateStatus(rejectionRate, "badHigh")} />
        <MetricCard label="Situations to review" value={formatNumber(problematicVisits)} detail="To review" status={problematicVisits > 0 ? "critical" : "success"} />
        <MetricCard label="Task Completion" value={`${taskCompletionRate}%`} detail="Tasks done / assigned" status={rateStatus(taskCompletionRate, "goodHigh")} />
        <MetricCard label="Active Merch" value={formatNumber(activeMerchandisers)} detail="In selected period" />
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
