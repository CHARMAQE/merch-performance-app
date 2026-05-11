import { ActivityIndicator, Pressable, ScrollView, Text, View } from "react-native";
import { API_BASE } from "../api/backendApi";
import { DashboardFilter } from "../components/DashboardFilter";
import { StatCard } from "../components/StatCard";
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
  onMonthChange,
  onDayChange,
  onRefresh,
  onLogout,
}) {
  const counts = overview?.tableCounts;
  const activity = overview?.storeActivity;
  const dailyReport = overview?.dailyReport;

  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>
      <View style={styles.headerRow}>
        <View style={styles.headerTitleBlock}>
          <Text style={styles.title}>Welcome {username}</Text>
        </View>
        <Pressable style={styles.logoutButton} onPress={onLogout}>
          <Text style={styles.logoutButtonText}>Logout</Text>
        </Pressable>
      </View>

      <DashboardFilter
        selectedMonth={selectedMonth}
        selectedDay={selectedDay}
        onMonthChange={onMonthChange}
        onDayChange={onDayChange}
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
        <StatCard
          label="Stores"
          value={formatNumber(activity?.visitedStores)}
        />
        <StatCard
          label="Field Visits"
          value={formatNumber(activity?.visits ?? counts?.visits)}
          detail={`${formatNumber(dailyReport?.storesRevisited)} stores revisited`}
        />
        <StatCard
          label="Merchandisers"
          value={formatNumber(dailyReport?.activeMerchandisers)}
        />
      </View>

      <Pressable style={styles.secondaryButton} onPress={onRefresh}>
        <Text style={styles.secondaryButtonText}>Refresh Data</Text>
      </Pressable>
    </ScrollView>
  );
}
