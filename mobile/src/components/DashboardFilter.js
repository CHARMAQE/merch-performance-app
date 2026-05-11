import { Pressable, ScrollView, Text, View } from "react-native";
import { DAYS, MONTHS } from "../constants/appConstants";
import { styles } from "../styles/appStyles";
import { FilterChip } from "./FilterChip";

export function DashboardFilter({
  selectedMonth,
  selectedDay,
  onMonthChange,
  onDayChange,
  onApply,
}) {
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
