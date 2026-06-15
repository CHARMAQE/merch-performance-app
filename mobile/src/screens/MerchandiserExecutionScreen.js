import { useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import {
  getSupervisorMerchandiserStores,
} from "../api/backendApi";
import { REPORT_YEAR } from "../constants/appConstants";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatNumber } from "../utils/formatters";
import { sortBySearchScore } from "../utils/search";

export function MerchandiserExecutionScreen({
  supervisorId,
  merchandisers = [],
  selectedMerchandiser,
  selectedMerchandiserStores = [],
  onSelectedMerchandiserChange,
  onSelectedMerchandiserStoresChange,
  onOpenTasksOverview,
  overview,
  isLoading,
  error,
  selectedMonth,
  selectedDay,
  startDate,
  endDate,
  storeFormatGroup = "ALL",
  onStoreFormatGroupChange,
}) {
  const [searchTerm, setSearchTerm] = useState("");
  const [isStoresLoading, setIsStoresLoading] = useState(false);
  const [storesError, setStoresError] = useState("");
  const filteredMerchandisers = useMemo(() => {
    if (!searchTerm.trim()) {
      return merchandisers;
    }

    return sortBySearchScore(merchandisers, searchTerm, (merchandiser) => [
      merchandiser.employeeCode,
      merchandiser.username,
      merchandiser.supervisorName,
      merchandiser.city,
      merchandiser.region,
      ...(merchandiser.cities || []),
    ]);
  }, [merchandisers, searchTerm]);
  const isSearchMode = Boolean(searchTerm.trim());

  function handleSearchChange(value) {
    setSearchTerm(value);

    if (selectedMerchandiser && value.trim()) {
      onSelectedMerchandiserChange?.(null);
      onSelectedMerchandiserStoresChange?.([]);
      setStoresError("");
    }

  }

  async function openMerchandiserStores(merchandiser) {
    if (!supervisorId || !merchandiser?.employeeCode) {
      return;
    }

    try {
      onSelectedMerchandiserChange?.(merchandiser);
      setIsStoresLoading(true);
      setStoresError("");

      const stores = await getSupervisorMerchandiserStores(
        supervisorId,
        merchandiser.employeeCode,
        {
          ...(startDate || endDate
            ? { startDate: startDate || undefined, endDate: endDate || undefined }
            : { year: REPORT_YEAR, month: selectedMonth, day: selectedDay }),
          storeFormatGroup,
        }
      );

      onSelectedMerchandiserStoresChange?.(Array.isArray(stores) ? stores : []);
    } catch (storeLoadError) {
      onSelectedMerchandiserStoresChange?.([]);
      setStoresError("Unable to load stores for this merchandiser.");
    } finally {
      setIsStoresLoading(false);
    }
  }

  function closeMerchandiserStores() {
    onSelectedMerchandiserChange?.(null);
    onSelectedMerchandiserStoresChange?.([]);
    setStoresError("");
  }

  const selectedPeriodLabel = buildSelectedPeriodLabel({
    startDate,
    endDate,
    selectedMonth,
    selectedDay,
  });

  return (
    <View style={styles.merchScreen}>
      {!selectedMerchandiser ? (
        <View style={styles.merchTopSearchPanel}>
          <View style={styles.merchSearchBox}>
            <Text style={styles.merchSearchLabel}>Search merchandiser</Text>
            <TextInput
              style={styles.merchTopSearchInput}
              value={searchTerm}
              onChangeText={handleSearchChange}
              placeholder="Name, code, or city"
              placeholderTextColor={colors.muted}
              autoCapitalize="none"
              autoCorrect={false}
            />
            {searchTerm ? (
              <Pressable
                style={styles.merchSearchClear}
                onPress={() => {
                  setSearchTerm("");
                }}
              >
                <Text style={styles.merchSearchClearText}>Clear</Text>
              </Pressable>
            ) : null}
          </View>
        </View>
      ) : null}

      <ScrollView
        contentContainerStyle={[
          styles.merchScrollContent,
          selectedMerchandiser ? styles.merchSelectedScrollContent : null,
        ]}
      >
        {selectedMerchandiser ? (
          <>
            <View style={styles.compactNavbar}>
              <Pressable style={styles.compactBackButton} onPress={closeMerchandiserStores}>
                <Text style={styles.compactBackIcon}>{"‹"}</Text>
              </Pressable>
              <Text style={styles.compactNavTitle}>Merch Execution</Text>
              <View style={styles.compactNavSpacer} />
            </View>

            <View style={styles.merchSelectedProfileCard}>
              <View style={styles.merchSelectedProfileHeader}>
                <View style={styles.merchSelectedAvatar}>
                  <Text style={styles.merchSelectedAvatarText}>
                    {getInitials(selectedMerchandiser.username || selectedMerchandiser.employeeCode)}
                  </Text>
                </View>
                <View style={styles.merchSelectedProfileTextBlock}>
                  <Text style={styles.merchSelectedProfileLabel}>Merchandiser</Text>
                  <Text style={styles.merchSelectedProfileName} numberOfLines={2}>
                    {selectedMerchandiser.username || "Unknown merchandiser"}
                  </Text>
                  <Text style={styles.merchSelectedProfileMeta}>
                    Employee code: {selectedMerchandiser.employeeCode || "--"}
                  </Text>
                </View>
              </View>
              {selectedMerchandiser.city || selectedMerchandiser.region ? (
                <Text style={styles.merchSelectedProfileMeta} numberOfLines={2}>
                  {[selectedMerchandiser.city, selectedMerchandiser.region]
                    .filter(Boolean)
                    .join(" - ")}
                </Text>
              ) : null}
              <Text style={styles.merchSelectedProfilePeriod}>{selectedPeriodLabel}</Text>
            </View>
          </>
        ) : (
          <View style={styles.merchMainHeader}>
            {isSearchMode ? (
              <Text style={styles.merchMainSubtitle}>
                {formatNumber(filteredMerchandisers.length)} merchandisers found
              </Text>
            ) : null}
          </View>
        )}

        {isLoading ? (
          <View style={styles.inlineState}>
            <ActivityIndicator color={colors.navy} />
            <Text style={styles.bodyText}>Loading merchandisers...</Text>
          </View>
        ) : null}

        {error ? <Text style={styles.errorText}>{error}</Text> : null}

        {selectedMerchandiser ? (
          <>
            {isStoresLoading ? (
              <View style={styles.inlineState}>
                <ActivityIndicator color={colors.navy} />
                <Text style={styles.bodyText}>Loading stores...</Text>
              </View>
            ) : null}

            {storesError ? <Text style={styles.errorText}>{storesError}</Text> : null}

            {selectedMerchandiserStores.length > 0 ? (
              <View style={styles.merchStoreList}>
                {selectedMerchandiserStores.map((store, index) => (
                  <StoreVisitCard
                    key={`${store.visitId || store.storeCode}-${store.visitDate}-${index}`}
                    selectedMerchandiser={selectedMerchandiser}
                    store={store}
                    onOpenTasksOverview={onOpenTasksOverview}
                  />
                ))}
              </View>
            ) : !isStoresLoading ? (
              <Text style={styles.bodyText}>No stores found for this filter.</Text>
            ) : null}
          </>
        ) : null}

        {!selectedMerchandiser ? (
          <>
            <View style={styles.statusFilterRow}>
              {["all", "gt", "mt"].map((channel) => (
                <Pressable
                  key={channel}
                  style={[
                    styles.statusFilterChip,
                    normalizeStoreFormatGroup(storeFormatGroup) === channel.toUpperCase()
                      ? styles.statusFilterChipActive
                      : null,
                  ]}
                  onPress={() => onStoreFormatGroupChange?.(channel.toUpperCase())}
                >
                  <Text
                    style={[
                      styles.statusFilterText,
                      normalizeStoreFormatGroup(storeFormatGroup) === channel.toUpperCase()
                        ? styles.statusFilterTextActive
                        : null,
                    ]}
                  >
                    {channelFilterLabel(channel)}
                  </Text>
                </Pressable>
              ))}
            </View>

            {filteredMerchandisers.length > 0 ? (
              <View style={styles.merchList}>
                {filteredMerchandisers.map((merchandiser) => (
                  <MerchandiserCard
                    key={merchandiser.employeeCode}
                    merchandiser={merchandiser}
                    onPress={() => openMerchandiserStores(merchandiser)}
                  />
                ))}
              </View>
            ) : (
              <Text style={styles.bodyText}>No merchandiser found for this filter.</Text>
            )}
          </>
        ) : null}
      </ScrollView>
    </View>
  );
}

function MerchandiserCard({ merchandiser, onPress }) {
  const channelLabel = getMerchChannelLabel(merchandiser);
  const locationLabel = getMerchLocationLabel(merchandiser);

  return (
    <Pressable onPress={onPress} style={styles.merchandiserCard}>
      <View style={styles.merchCardHeader}>
        <View style={styles.merchTitleBlock}>
          <Text style={styles.merchName} numberOfLines={1}>
            {merchandiser.username || "Unknown merchandiser"}
          </Text>
          <Text style={styles.merchCode}>
            {merchandiser.employeeCode || "No employee code"}
          </Text>
        </View>
        {channelLabel ? (
          <View style={styles.merchChannelChip}>
            <Text style={styles.merchChannelChipText}>{channelLabel}</Text>
          </View>
        ) : null}
      </View>

      {merchandiser.supervisorName ? (
        <Text style={styles.merchCardMeta} numberOfLines={1}>
          Supervisor: {merchandiser.supervisorName}
        </Text>
      ) : null}

      <View style={styles.merchMetricGrid}>
        <MerchMetricPill label="Planned" value={merchandiser.plannedVisits} />
        <MerchMetricPill
          label="Executed"
          value={merchandiser.executedVisits ?? merchandiser.storesCovered}
        />
        <MerchMetricPill label="Deviation" value={merchandiser.deviationVisits ?? 0} />
      </View>

      <Text style={styles.merchCities} numberOfLines={2}>
        {locationLabel}
      </Text>
    </Pressable>
  );
}

function MerchMetricPill({ label, value }) {
  return (
    <View style={styles.merchMetricPill}>
      <Text style={styles.merchMetricPillLabel}>{label}:</Text>
      <Text style={styles.merchMetricPillValue}>{formatNumber(value)}</Text>
    </View>
  );
}

function StoreVisitCard({ onOpenTasksOverview, selectedMerchandiser, store }) {
  const hasDeviation = Boolean(store?.hasDeviation ?? store?.deviation);
  const deviationReason = hasDeviation ? store?.deviationReason : null;
  const visitTypeLabel = storeVisitTypeLabel(store);
  const isCovered = Boolean(store?.visitId || store?.isDone || store?.covered);
  const statusType = hasDeviation ? "deviation" : isCovered ? "covered" : null;

  return (
    <Pressable
      style={[
        styles.merchStoreVisitCard,
        store?.visitId ? styles.merchStoreCardPressable : styles.disabledButton,
      ]}
      disabled={!store?.visitId}
      onPress={() => onOpenTasksOverview?.(store, selectedMerchandiser)}
    >
      <View style={styles.storeRow}>
        <View style={styles.merchTitleBlock}>
          <Text style={styles.merchName} numberOfLines={1}>
            {store.storeName || "Unknown store"}
          </Text>
          <Text style={styles.merchCode}>
            {[store.storeCode, store.storeFormat].filter(Boolean).join(" - ")}
          </Text>
        </View>
      </View>

      <Text style={styles.merchCities} numberOfLines={2}>
        {[store.storeCity, store.storeRegion, formatReadableDate(store.visitDate)]
          .filter(Boolean)
          .join(" - ")}
      </Text>

      {visitTypeLabel ? (
        <View style={styles.visitTypeLine}>
          <Text style={styles.visitTypeLabel}>Visit type:</Text>
          <Text style={styles.visitTypeValue}>{visitTypeLabel}</Text>
        </View>
      ) : null}

      {statusType ? (
        <View style={styles.storeStatusChipRow}>
          <Text style={styles.storeStatusLabel}>Status:</Text>
          <View
            style={[
              styles.storeStatusChip,
              statusType === "deviation"
                ? styles.storeStatusChipDeviation
                : styles.storeStatusChipCovered,
            ]}
          >
            <Text
              style={[
                styles.storeStatusChipText,
                statusType === "deviation"
                  ? styles.storeStatusChipTextDeviation
                  : styles.storeStatusChipTextCovered,
              ]}
            >
              {statusType === "deviation" ? "Deviation" : "Covered"}
            </Text>
          </View>
        </View>
      ) : null}

      {hasDeviation ? (
        <View style={styles.deviationBlock}>
          {deviationReason ? (
            <Text style={styles.deviationReasonText} numberOfLines={2}>
              Reason: {deviationReason}
            </Text>
          ) : null}
        </View>
      ) : null}

      <View style={styles.storeTapHintRow}>
        {store?.visitId ? (
          <Text style={styles.storeTapHintText}>Tap to view execution</Text>
        ) : null}
      </View>
    </Pressable>
  );
}

function channelFilterLabel(channel) {
  if (channel === "gt") return "GT";
  if (channel === "mt") return "MT";
  return "All";
}

function storeVisitTypeLabel(store) {
  if (store?.isAdhoc) return "Adhoc";
  if (store?.isPlanned) return "Planned";
  return null;
}

function getMerchChannelLabel(merchandiser) {
  return getMerchChannels(merchandiser).join(" / ");
}

function getMerchChannels(merchandiser) {
  const formats = Array.isArray(merchandiser.storeFormats)
    ? merchandiser.storeFormats
    : merchandiser.storeFormat
      ? [merchandiser.storeFormat]
      : [];

  const hasGt = formats.some((format) => String(format).trim().toUpperCase() === "GROCERY");
  const hasMt = formats.some((format) => {
    const normalized = String(format).trim().toUpperCase();
    return normalized && normalized !== "GROCERY";
  });

  return [hasGt ? "GT" : null, hasMt ? "MT" : null].filter(Boolean);
}

function getMerchLocationLabel(merchandiser) {
  return [merchandiser.city || merchandiser.region, ...(merchandiser.cities || [])]
    .filter(Boolean)
    .join(" - ") || "No city captured";
}

function normalizeStoreFormatGroup(value) {
  if (value === "GT" || value === "MT") {
    return value;
  }

  return "ALL";
}

function buildSelectedPeriodLabel({ endDate, selectedDay, selectedMonth, startDate }) {
  if (startDate && endDate) {
    if (startDate === endDate) {
      return formatReadableDate(startDate);
    }

    return `${formatReadableDate(startDate)} - ${formatReadableDate(endDate)}`;
  }

  if (startDate || endDate) {
    return formatReadableDate(startDate || endDate);
  }

  if (selectedMonth && selectedDay) {
    return formatReadableDateFromParts(REPORT_YEAR, selectedMonth, selectedDay);
  }

  if (selectedMonth) {
    return formatReadableMonth(REPORT_YEAR, selectedMonth);
  }

  return "Selected period";
}

function formatReadableDate(value) {
  if (!value) {
    return "";
  }

  const [year, month, day] = String(value).split("-").map((part) => Number(part));
  if (!year || !month || !day) {
    return String(value);
  }

  return formatReadableDateFromParts(year, month, day);
}

function formatReadableDateFromParts(year, month, day) {
  const monthName = MONTH_NAMES[Number(month) - 1];
  if (!monthName || !day) {
    return "Selected period";
  }

  return `${String(day).padStart(2, "0")} ${monthName} ${year}`;
}

function formatReadableMonth(year, month) {
  const monthName = MONTH_NAMES[Number(month) - 1];
  return monthName ? `${monthName} ${year}` : "Selected period";
}

function getInitials(value) {
  return String(value || "M")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part.charAt(0).toUpperCase())
    .join("") || "M";
}

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];
