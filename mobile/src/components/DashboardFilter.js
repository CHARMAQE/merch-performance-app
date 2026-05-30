import { Pressable, ScrollView, Text, TextInput, View } from "react-native";
import { colors } from "../constants/colors";
import { DAYS, MONTHS, REPORT_YEAR } from "../constants/appConstants";
import { styles } from "../styles/appStyles";
import { FilterChip } from "./FilterChip";

export function DashboardFilter({
  selectedMonth,
  selectedDay,
  startDate,
  endDate,
  onMonthChange,
  onDayChange,
  onStartDateChange,
  onEndDateChange,
  onApply,
}) {
  const selectedMonthLabel =
    MONTHS.find((month) => month.value === selectedMonth)?.label || "Month";
  const periodLabel = buildPeriodLabel({
    endDate,
    selectedDay,
    selectedMonth,
    selectedMonthLabel,
    startDate,
  });

  return (
    <View style={styles.filterPanel}>
      <View style={styles.filterHeader}>
        <View>
          <Text style={styles.eyebrow}>Filter Data</Text>
          <Text style={styles.filterTitle}>Month and day</Text>
        </View>
        <Pressable style={styles.applyButton} onPress={onApply}>
          <Text style={styles.applyButtonText}>Apply</Text>
        </Pressable>
      </View>

      <Text style={styles.selectedPeriodText}>{periodLabel}</Text>

      <Text style={styles.filterLabel}>Date range</Text>
      <View style={styles.dateRangeRow}>
        <TextInput
          value={startDate}
          onChangeText={onStartDateChange}
          placeholder="YYYY-MM-DD"
          placeholderTextColor={colors.muted}
          autoCapitalize="none"
          style={styles.dateInput}
        />
        <TextInput
          value={endDate}
          onChangeText={onEndDateChange}
          placeholder="YYYY-MM-DD"
          placeholderTextColor={colors.muted}
          autoCapitalize="none"
          style={styles.dateInput}
        />
      </View>

      <Text style={styles.filterLabel}>Month</Text>
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.filterRow}
      >
        {MONTHS.map((month) => (
          <FilterChip
            key={month.value}
            label={month.label}
            isActive={selectedMonth === month.value}
            onPress={() => onMonthChange(month.value)}
          />
        ))}
      </ScrollView>

      <Text style={styles.filterLabel}>Day</Text>
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.filterRow}
      >
        <FilterChip
          label="All"
          isActive={!selectedDay}
          onPress={() => onDayChange(null)}
        />
        {DAYS.map((day) => (
          <FilterChip
            key={day}
            label={String(day)}
            isActive={selectedDay === day}
            onPress={() => onDayChange(day)}
          />
        ))}
      </ScrollView>
    </View>
  );
}

function buildPeriodLabel({ endDate, selectedDay, selectedMonth, selectedMonthLabel, startDate }) {
  if (startDate || endDate) {
    return `Selected period: ${startDate || "start date"} to ${endDate || "end date"}`;
  }

  if (selectedDay) {
    return `Selected day: ${REPORT_YEAR}-${pad2(selectedMonth)}-${pad2(selectedDay)}`;
  }

  return `Selected period: ${selectedMonthLabel} ${REPORT_YEAR}`;
}

function pad2(value) {
  return String(value).padStart(2, "0");
}
