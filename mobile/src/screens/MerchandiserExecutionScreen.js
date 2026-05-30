import { useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import {
  getSupervisorMerchandiserStores,
} from "../api/backendApi";
import { REPORT_YEAR } from "../constants/appConstants";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate, formatNumber } from "../utils/formatters";
import { sortBySearchScore } from "../utils/search";

export function MerchandiserExecutionScreen({
  supervisorId,
  merchandisers = [],
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
  const [storeStatusFilter, setStoreStatusFilter] = useState("all");
  const [selectedMerchandiser, setSelectedMerchandiser] = useState(null);
  const [selectedMerchandiserStores, setSelectedMerchandiserStores] = useState([]);
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
  const filteredSelectedStores = useMemo(
    () =>
      selectedMerchandiserStores.filter(
        (store) => storeStatusFilter === "all" || getStoreStatusKey(store) === storeStatusFilter
      ),
    [selectedMerchandiserStores, storeStatusFilter]
  );
  const isSearchMode = Boolean(searchTerm.trim());

  function handleSearchChange(value) {
    setSearchTerm(value);

    if (selectedMerchandiser && value.trim()) {
      setSelectedMerchandiser(null);
      setSelectedMerchandiserStores([]);
      setStoresError("");
    }

  }

  async function openMerchandiserStores(merchandiser) {
    if (!supervisorId || !merchandiser?.employeeCode) {
      return;
    }

    try {
      setSelectedMerchandiser(merchandiser);
      setStoreStatusFilter("all");
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

      setSelectedMerchandiserStores(Array.isArray(stores) ? stores : []);
    } catch (storeLoadError) {
      setSelectedMerchandiserStores([]);
      setStoresError("Unable to load stores for this merchandiser.");
    } finally {
      setIsStoresLoading(false);
    }
  }

  function closeMerchandiserStores() {
    setSelectedMerchandiser(null);
    setSelectedMerchandiserStores([]);
    setStoreStatusFilter("all");
    setStoresError("");
  }

  return (
    <View style={styles.merchScreen}>
      <View style={styles.merchTopSearchPanel}>
        {selectedMerchandiser ? (
          <Pressable
            style={styles.merchTopBackNav}
            onPress={closeMerchandiserStores}
          >
            <Text style={styles.merchTopBackIcon}>{"<"}</Text>
            <Text style={styles.merchTopBackText}>Merch</Text>
          </Pressable>
        ) : (
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
        )}
      </View>

      <ScrollView contentContainerStyle={styles.merchScrollContent}>
        {selectedMerchandiser ? (
          <View style={styles.merchSearchModeHeader}>
            <Text style={styles.eyebrow}>STORES</Text>
            <Text style={styles.title}>{selectedMerchandiser.username}</Text>
            <Text style={styles.bodyText}>{selectedMerchandiser.employeeCode}</Text>
          </View>
        ) : isSearchMode ? (
          <View style={styles.merchSearchModeHeader}>
            <Text style={styles.eyebrow}>SEARCH RESULTS</Text>
            <Text style={styles.title}>{formatNumber(filteredMerchandisers.length)} Merch</Text>
            <Text style={styles.bodyText}>Results matching name, code, or city.</Text>
          </View>
        ) : (
          <View>
            <Text style={styles.eyebrow}>MERCH</Text>
            <Text style={styles.title}>Execution</Text>
            <Text style={styles.bodyText}>Merchandiser activity for the selected dashboard filter.</Text>
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
            <View style={styles.statusFilterRow}>
              {["all", "covered", "deviation", "nonVisited", "rejected"].map((status) => (
                <Pressable
                  key={status}
                  style={[
                    styles.statusFilterChip,
                    storeStatusFilter === status ? styles.statusFilterChipActive : null,
                  ]}
                  onPress={() => setStoreStatusFilter(status)}
                >
                  <Text
                    style={[
                      styles.statusFilterText,
                      storeStatusFilter === status ? styles.statusFilterTextActive : null,
                    ]}
                  >
                    {storeStatusFilterLabel(status)}
                  </Text>
                </Pressable>
              ))}
            </View>

            {isStoresLoading ? (
              <View style={styles.inlineState}>
                <ActivityIndicator color={colors.navy} />
                <Text style={styles.bodyText}>Loading stores...</Text>
              </View>
            ) : null}

            {storesError ? <Text style={styles.errorText}>{storesError}</Text> : null}

            {filteredSelectedStores.length > 0 ? (
              <View style={styles.merchStoreList}>
                {filteredSelectedStores.map((store, index) => (
                  <View
                    key={`${store.storeCode}-${store.visitDate}-${index}`}
                    style={styles.storeCard}
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
                      <View style={[styles.statusBadge, storeStatusStyle(getStoreStatusKey(store))]}>
                        <Text style={styles.statusBadgeText}>{store.executionStatus || "Covered"}</Text>
                      </View>
                    </View>

                    <Text style={styles.merchCities} numberOfLines={2}>
                      {[store.storeCity, store.storeRegion, formatDate(store.visitDate)]
                        .filter(Boolean)
                        .join(" - ")}
                    </Text>
                    <View style={styles.merchMetricRow}>
                      <Text style={styles.merchMetric}>{storeVisitTypeLabel(store)}</Text>
                      {store.callCycleType ? (
                        <Text style={styles.merchMetric}>{store.callCycleType}</Text>
                      ) : null}
                    </View>
                    {selectedMerchandiser.username || selectedMerchandiser.supervisorName ? (
                      <Text style={styles.merchMetric}>
                        {[selectedMerchandiser.username, selectedMerchandiser.supervisorName]
                          .filter(Boolean)
                          .join(" - ")}
                      </Text>
                    ) : null}
                    {store.deviationReason ? (
                      <Text style={styles.merchReasons} numberOfLines={2}>
                        Reason: {store.deviationReason}
                      </Text>
                    ) : null}
                    {shouldShowTaskCompletion(store.taskCompletionRate) ? (
                      <Text style={styles.merchReasons}>
                        Task Completion {Math.round(Number(store.taskCompletionRate || 0))}%
                      </Text>
                    ) : null}
                  </View>
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
                  <Pressable
                    key={merchandiser.employeeCode}
                    onPress={() => openMerchandiserStores(merchandiser)}
                    style={styles.storeCard}
                  >
                    <View style={styles.storeRow}>
                      <View style={styles.merchTitleBlock}>
                        <Text style={styles.merchName} numberOfLines={1}>
                          {merchandiser.username || "Unknown merchandiser"}
                        </Text>
                        <Text style={styles.merchCode}>
                          {merchandiser.employeeCode || "No employee code"}
                        </Text>
                        {merchandiser.supervisorName ? (
                          <Text style={styles.merchCode} numberOfLines={1}>
                            Supervisor: {merchandiser.supervisorName}
                          </Text>
                        ) : null}
                        {getMerchChannelLabel(merchandiser) ? (
                          <Text style={styles.merchCode}>
                            {getMerchChannelLabel(merchandiser)}
                          </Text>
                        ) : null}
                      </View>
                    </View>

                    <View style={styles.merchMetricRow}>
                      <Text style={styles.merchMetric}>
                        Planned: {formatNumber(merchandiser.plannedVisits)}
                      </Text>
                      {Number(merchandiser.adhocVisits || 0) > 0 ? (
                        <Text style={styles.merchMetric}>
                          Adhoc: {formatNumber(merchandiser.adhocVisits)}
                        </Text>
                      ) : null}
                      <Text style={styles.merchMetric}>
                        Executed: {formatNumber(merchandiser.executedVisits ?? merchandiser.storesCovered)}
                      </Text>
                    </View>
                    <View style={styles.merchMetricRow}>
                      <Text style={styles.merchMetric}>
                        Non Visited: {formatNumber(merchandiser.nonVisitedVisits)}
                      </Text>
                      <Text style={styles.merchMetric}>
                        Dev: {formatNumber(merchandiser.deviationVisits)}
                      </Text>
                      <Text style={styles.merchMetric}>
                        Reject: {formatNumber(merchandiser.rejectedVisits)}
                      </Text>
                    </View>

                    <Text style={styles.merchCities} numberOfLines={2}>
                      {[merchandiser.city || merchandiser.region, ...(merchandiser.cities || [])]
                        .filter(Boolean)
                        .join(" - ") || "No city captured"}
                    </Text>
                  </Pressable>
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

function storeStatusStyle(status) {
  if (status === "nonVisited") return styles.statusBadgeNonVisited;
  if (status === "deviation") return styles.statusBadgeDeviation;
  if (status === "rejected") return styles.statusBadgeRejected;
  return styles.statusBadgeCovered;
}

function channelFilterLabel(channel) {
  if (channel === "gt") return "GT";
  if (channel === "mt") return "MT";
  return "All";
}

function storeStatusFilterLabel(status) {
  if (status === "covered") return "Covered";
  if (status === "deviation") return "Deviation";
  if (status === "nonVisited") return "Non Visited";
  if (status === "rejected") return "Rejected";
  return "All";
}

function getStoreStatusKey(store) {
  if (store?.executionStatus === "Rejected") return "rejected";
  if (store?.executionStatus === "Deviation") return "deviation";
  if (store?.executionStatus === "Non Visited") return "nonVisited";
  return "covered";
}

function storeVisitTypeLabel(store) {
  if (store?.isAdhoc) return "Adhoc";
  if (store?.isPlanned) return "Planned";
  return "Visit type not available";
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

function normalizeStoreFormatGroup(value) {
  if (value === "GT" || value === "MT") {
    return value;
  }

  return "ALL";
}

function shouldShowTaskCompletion(value) {
  if (value === null || value === undefined || value === "") {
    return false;
  }

  return Number(value) < 100;
}
