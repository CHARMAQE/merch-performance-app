import { useEffect, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, Text, View } from "react-native";
import { getStoreDetail } from "../api/backendApi";
import { REPORT_YEAR } from "../constants/appConstants";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate } from "../utils/formatters";

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

  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>
      <View style={styles.compactNavbar}>
        <Pressable style={styles.compactBackButton} onPress={onBack}>
          <Text style={styles.compactBackIcon}>{"‹"}</Text>
        </Pressable>
        <Text style={styles.compactNavTitle}>Stores</Text>
        <View style={styles.compactNavSpacer} />
      </View>

      <View style={styles.storeDetailHero}>
        <Text style={styles.storeDetailHeroLabel}>STORE DETAILS</Text>
        <Text style={styles.storeDetailHeroTitle} numberOfLines={2}>
          {details?.storeName || store?.storeName || "Store"}
        </Text>
        <Text style={styles.storeDetailHeroCode}>
          Store code: {displayValue(details?.storeCode || storeCode)}
        </Text>
        <View style={styles.storeDetailHeroChipRow}>
          <View style={styles.storeDetailHeroChip}>
            <Text style={styles.storeDetailHeroChipText}>
              {displayValue(details?.storeFormat || store?.storeFormat)}
            </Text>
          </View>
        </View>
        <Text style={styles.storeDetailHeroMeta} numberOfLines={2}>
          {[
            details?.city || details?.storeCity || store?.city || store?.storeCity,
            details?.region || details?.storeRegion || store?.region || store?.storeRegion,
          ]
            .filter(isPresent)
            .join(" - ") || "--"}
        </Text>
      </View>

      {isLoading ? (
        <View style={styles.inlineState}>
          <ActivityIndicator color={colors.navy} />
          <Text style={styles.bodyText}>Loading store details...</Text>
        </View>
      ) : null}

      {error ? <Text style={styles.errorText}>{error}</Text> : null}

      <View style={styles.storeDetailInfoGrid}>
        <DetailMetric label="Visit date" value={formatSafeDate(details?.visitDate)} />
        <DetailMetric
          label="Latest visit"
          value={formatSafeDate(details?.latestVisitDate || details?.visitDate)}
        />
        <DetailMetric
          label="Merchandiser"
          value={displayValue(details?.merchandiserName || details?.username)}
        />
        <DetailMetric label="Supervisor" value={displayValue(details?.supervisorName)} />
        <DetailMetric
          label="City"
          value={displayValue(details?.city || details?.storeCity || store?.city || store?.storeCity)}
        />
        <DetailMetric
          label="Region"
          value={displayValue(details?.region || details?.storeRegion || store?.region || store?.storeRegion)}
        />
        <DetailMetric label="Format" value={displayValue(details?.storeFormat || store?.storeFormat)} />
      </View>
    </ScrollView>
  );
}

function DetailMetric({ label, value }) {
  return (
    <View style={styles.storeDetailInfoCard}>
      <Text style={styles.storeDetailInfoLabel}>{label}</Text>
      <Text style={styles.storeDetailInfoValue} numberOfLines={2}>
        {value}
      </Text>
    </View>
  );
}

function displayValue(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return "--";
  }

  return String(value);
}

function formatSafeDate(value) {
  if (!value) {
    return "--";
  }

  return formatDate(value);
}

function isPresent(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}
