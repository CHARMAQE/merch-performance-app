import { useEffect, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, Text, View } from "react-native";
import { getStoreDetail } from "../api/backendApi";
import { REPORT_YEAR } from "../constants/appConstants";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate, formatNumber, formatPercentage } from "../utils/formatters";

export function StoreDetailScreen({
  supervisorId,
  store,
  selectedMonth,
  selectedDay,
  startDate,
  endDate,
  onBack,
}) {
  const [details, setDetails] = useState(store || null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");
  const storeCode = store?.storeCode;

  useEffect(() => {
    let isMounted = true;

    async function loadDetails() {
      if (!storeCode) {
        return;
      }

      try {
        setIsLoading(true);
        setError("");
        const result = await getStoreDetail(storeCode, {
          supervisorId,
          ...(startDate || endDate
            ? { startDate: startDate || undefined, endDate: endDate || undefined }
            : { year: REPORT_YEAR, month: selectedMonth, day: selectedDay }),
        });

        if (isMounted) {
          setDetails(result);
        }
      } catch (loadError) {
        if (isMounted) {
          setError("Unable to load data. Please check backend connection.");
        }
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    }

    loadDetails();

    return () => {
      isMounted = false;
    };
  }, [endDate, selectedDay, selectedMonth, startDate, storeCode, supervisorId]);

  const status = getStatus(details);
  const startDistance = Number(details?.startDistanceMeters);
  const endDistance = Number(details?.endDistanceMeters);
  const hasGpsFlag = startDistance > 250 || endDistance > 250;

  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>
      <Pressable style={styles.merchTopBackNav} onPress={onBack}>
        <Text style={styles.merchTopBackIcon}>{"<"}</Text>
        <Text style={styles.merchTopBackText}>Stores</Text>
      </Pressable>

      <View style={styles.panel}>
        <View style={styles.panelHeaderRow}>
          <View style={styles.headerTitleBlock}>
            <Text style={styles.eyebrow}>STORE DETAILS</Text>
            <Text style={styles.title}>{details?.storeName || store?.storeName || "Store"}</Text>
            <Text style={styles.bodyText}>{details?.storeCode || storeCode}</Text>
          </View>
          <View style={[styles.statusBadge, status.style]}>
            <Text style={styles.statusBadgeText}>{status.label}</Text>
          </View>
        </View>

        {isLoading ? (
          <View style={styles.inlineState}>
            <ActivityIndicator color={colors.navy} />
            <Text style={styles.bodyText}>Loading store details...</Text>
          </View>
        ) : null}

        {error ? <Text style={styles.errorText}>{error}</Text> : null}

        <View style={styles.storeMetricGrid}>
          <DetailMetric label="City" value={details?.city || details?.storeCity || "Not available"} />
          <DetailMetric label="Region" value={details?.region || details?.storeRegion || "Not available"} />
          <DetailMetric label="Format" value={details?.storeFormat || "Not available"} />
          <DetailMetric label="Visit date" value={formatDate(details?.visitDate)} />
          <DetailMetric label="Latest visit" value={formatDate(details?.latestVisitDate || details?.visitDate)} />
          <DetailMetric label="Merchandiser" value={details?.username || "Not available"} />
          <DetailMetric label="Employee code" value={details?.employeeCode || "Not available"} />
          <DetailMetric label="Supervisor" value={details?.supervisorName || "Not available"} />
          <DetailMetric label="Task assigned" value={formatNumber(details?.taskAssigned)} />
          <DetailMetric label="Task done" value={formatNumber(details?.taskDone)} />
          <DetailMetric label="Task completion" value={formatPercentage(details?.taskPer)} />
          <DetailMetric
            label="GPS distance"
            value={[
              formatDistance("Start", details?.startDistanceMeters),
              formatDistance("End", details?.endDistanceMeters),
            ].join(" / ")}
            warning={hasGpsFlag}
          />
          <DetailMetric label="User attendance" value={details?.userAttendance || "Not available"} />
          <DetailMetric label="Superior attendance" value={details?.superiorAttendance || "Not available"} />
          <DetailMetric label="Final attendance" value={details?.finalUserAttendance || "Not available"} />
          <DetailMetric label="Reason" value={details?.reason || "Not available"} />
        </View>
      </View>
    </ScrollView>
  );
}

function DetailMetric({ label, value, warning }) {
  return (
    <View style={[styles.storeMetric, warning ? styles.storeMetricWarning : null]}>
      <Text style={styles.storeMetricLabel}>{label}</Text>
      <Text style={styles.storeMetricValue}>{value}</Text>
    </View>
  );
}

function getStatus(store) {
  if (store?.rejection) return { label: "Rejected", style: styles.statusBadgeRejected };
  if (store?.deviation) return { label: "Deviation", style: styles.statusBadgeDeviation };
  if (store?.notVisited) return { label: "Non Visited", style: styles.statusBadgeNonVisited };
  return { label: "Covered", style: styles.statusBadgeCovered };
}

function formatDistance(label, value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return `${label}: Not available`;
  }

  return `${label}: ${Math.round(Number(value))}m`;
}
