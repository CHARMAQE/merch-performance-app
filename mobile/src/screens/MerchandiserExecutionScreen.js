import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, Text, TextInput, View } from "react-native";
import {
  getSupervisorExecutionStores,
  getSupervisorMerchandiserStores,
} from "../api/backendApi";
import { MONTHS, REPORT_YEAR } from "../constants/appConstants";
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
}) {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedMerchandiser, setSelectedMerchandiser] = useState(null);
  const [selectedMerchandiserStores, setSelectedMerchandiserStores] = useState([]);
  const [selectedExecutionType, setSelectedExecutionType] = useState("");
  const [coveredStores, setCoveredStores] = useState([]);
  const [deviationStores, setDeviationStores] = useState([]);
  const [isExecutionStoresLoading, setIsExecutionStoresLoading] = useState(false);
  const [executionStoresError, setExecutionStoresError] = useState("");
  const [isStoresLoading, setIsStoresLoading] = useState(false);
  const [storesError, setStoresError] = useState("");
  const totalCoveredExecutions = coveredStores.reduce(
    (total, store) => total + (store.executionCount || 0),
    0
  );
  const totalDeviationExecutions = deviationStores.reduce(
    (total, store) => total + (store.executionCount || 0),
    0
  );
  const selectedExecutionStores =
    selectedExecutionType === "covered" ? coveredStores : deviationStores;
  const selectedExecutionTitle =
    selectedExecutionType === "covered" ? "Covered Stores" : "Deviation Stores";
  const selectedExecutionCountLabel =
    selectedExecutionType === "covered" ? "Covered count" : "Deviation count";
  const selectedMonthLabel =
    MONTHS.find((monthOption) => monthOption.value === selectedMonth)?.label || "Month";
  const periodLabel = selectedDay
    ? `${selectedMonthLabel} 1 to ${selectedMonthLabel} ${selectedDay}`
    : selectedMonthLabel;
  const filteredMerchandisers = useMemo(() => {
    if (!searchTerm.trim()) {
      return merchandisers;
    }

    return sortBySearchScore(merchandisers, searchTerm, (merchandiser) => [
      merchandiser.employeeCode,
      merchandiser.username,
      ...(merchandiser.cities || []),
    ]);
  }, [merchandisers, searchTerm]);
  const isSearchMode = Boolean(searchTerm.trim());

  useEffect(() => {
    let isMounted = true;

    async function loadExecutionStores() {
      if (!supervisorId) {
        return;
      }

      try {
        setIsExecutionStoresLoading(true);
        setExecutionStoresError("");

        const filters = {
          year: REPORT_YEAR,
          month: selectedMonth,
          day: selectedDay,
        };
        const [coveredResult, deviationResult] = await Promise.all([
          getSupervisorExecutionStores(supervisorId, "covered", filters),
          getSupervisorExecutionStores(supervisorId, "deviation", filters),
        ]);

        if (isMounted) {
          setCoveredStores(Array.isArray(coveredResult) ? coveredResult : []);
          setDeviationStores(Array.isArray(deviationResult) ? deviationResult : []);
        }
      } catch (loadExecutionStoresError) {
        if (isMounted) {
          setCoveredStores([]);
          setDeviationStores([]);
          setExecutionStoresError("Unable to load covered/deviation store lists.");
        }
      } finally {
        if (isMounted) {
          setIsExecutionStoresLoading(false);
        }
      }
    }

    loadExecutionStores();

    return () => {
      isMounted = false;
    };
  }, [supervisorId, selectedMonth, selectedDay]);

  function handleSearchChange(value) {
    setSearchTerm(value);

    if (selectedMerchandiser && value.trim()) {
      setSelectedMerchandiser(null);
      setSelectedMerchandiserStores([]);
      setStoresError("");
    }

    if (selectedExecutionType && value.trim()) {
      setSelectedExecutionType("");
    }
  }

  async function openMerchandiserStores(merchandiser) {
    if (!supervisorId || !merchandiser?.employeeCode) {
      return;
    }

    try {
      setSelectedMerchandiser(merchandiser);
      setIsStoresLoading(true);
      setStoresError("");

      const stores = await getSupervisorMerchandiserStores(
        supervisorId,
        merchandiser.employeeCode,
        {
          year: REPORT_YEAR,
          month: selectedMonth,
          day: selectedDay,
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
    setStoresError("");
  }

  function openExecutionStores(type) {
    setSelectedMerchandiser(null);
    setSelectedMerchandiserStores([]);
    setStoresError("");
    setSearchTerm("");
    setSelectedExecutionType(type);
  }

  function closeExecutionStores() {
    setSelectedExecutionType("");
  }

  return (
    <View style={styles.merchScreen}>
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

      <ScrollView contentContainerStyle={styles.merchScrollContent}>
        {selectedMerchandiser ? (
          <View style={styles.merchSearchModeHeader}>
            <Pressable style={styles.merchBackNav} onPress={closeMerchandiserStores}>
              <Text style={styles.merchBackNavText}>Merch list</Text>
            </Pressable>
            <Text style={styles.eyebrow}>MERCH STORES</Text>
            <Text style={styles.title}>{selectedMerchandiser.username}</Text>
            <Text style={styles.bodyText}>{selectedMerchandiser.employeeCode}</Text>
          </View>
        ) : selectedExecutionType ? (
          <View style={styles.merchSearchModeHeader}>
            <Pressable style={styles.merchBackNav} onPress={closeExecutionStores}>
              <Text style={styles.merchBackNavText}>Merch overview</Text>
            </Pressable>
            <Text style={styles.eyebrow}>MONTH TO DATE</Text>
            <Text style={styles.title}>{selectedExecutionTitle}</Text>
            <Text style={styles.bodyText}>{periodLabel}</Text>
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
          <View style={styles.panel}>
            <View style={styles.panelHeaderRow}>
              <View>
                <Text style={styles.panelTitle}>Visited Stores</Text>
                <Text style={styles.panelSubtitle}>Covered stores and deviations for this merchandiser</Text>
              </View>
            </View>

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
                  <View
                    key={`${store.storeCode}-${store.visitDate}-${index}`}
                    style={[
                      styles.merchStoreCard,
                      store.executionStatus === "Deviation" ? styles.merchStoreCardWarning : null,
                    ]}
                  >
                    <View style={styles.merchHeaderRow}>
                      <View style={styles.merchTitleBlock}>
                        <Text style={styles.merchName} numberOfLines={1}>
                          {store.storeName || "Unknown store"}
                        </Text>
                        <Text style={styles.merchCode}>{store.storeCode}</Text>
                      </View>
                      <View
                        style={[
                          styles.statusPill,
                          store.executionStatus === "Deviation"
                            ? styles.statusPillWarning
                            : styles.statusPillGood,
                        ]}
                      >
                        <Text style={styles.statusPillText}>{store.executionStatus}</Text>
                      </View>
                    </View>

                    <Text style={styles.merchCities} numberOfLines={2}>
                      {[store.storeCity, store.storeFormat, formatDate(store.visitDate)]
                        .filter(Boolean)
                        .join(" - ")}
                    </Text>
                    {store.deviationReason ? (
                      <Text style={styles.merchReasons} numberOfLines={2}>
                        Reason: {store.deviationReason}
                      </Text>
                    ) : null}
                  </View>
                ))}
              </View>
            ) : !isStoresLoading ? (
              <Text style={styles.bodyText}>No store details found for this merchandiser.</Text>
            ) : null}
          </View>
        ) : selectedExecutionType ? (
          <View style={styles.panel}>
            <View style={styles.panelHeaderRow}>
              <View>
                <Text style={styles.panelTitle}>{selectedExecutionTitle}</Text>
                <Text style={styles.panelSubtitle}>
                  Stores sorted by execution count in the selected period
                </Text>
              </View>
              <View style={styles.reportBadge}>
                <Text style={styles.reportBadgeText}>
                  {formatNumber(selectedExecutionStores.length)}
                </Text>
              </View>
            </View>

            {isExecutionStoresLoading ? (
              <View style={styles.inlineState}>
                <ActivityIndicator color={colors.navy} />
                <Text style={styles.bodyText}>Loading store list...</Text>
              </View>
            ) : null}

            {executionStoresError ? <Text style={styles.errorText}>{executionStoresError}</Text> : null}

            {selectedExecutionStores.length > 0 ? (
              <View style={styles.merchStoreList}>
                {selectedExecutionStores.map((store) => (
                  <View
                    key={store.storeCode}
                    style={[
                      styles.merchStoreCard,
                      selectedExecutionType === "deviation"
                        ? styles.merchStoreCardWarning
                        : null,
                    ]}
                  >
                    <View style={styles.merchHeaderRow}>
                      <View style={styles.merchTitleBlock}>
                        <Text style={styles.merchName} numberOfLines={1}>
                          {store.storeName || "Unknown store"}
                        </Text>
                        <Text style={styles.merchCode}>{store.storeCode}</Text>
                      </View>
                      <View
                        style={[
                          styles.statusPill,
                          selectedExecutionType === "deviation"
                            ? styles.statusPillWarning
                            : styles.statusPillGood,
                        ]}
                      >
                        <Text style={styles.statusPillLabel}>{selectedExecutionCountLabel}</Text>
                        <Text style={styles.statusPillText}>
                          {formatNumber(store.executionCount)}
                        </Text>
                      </View>
                    </View>

                    <Text style={styles.merchCities} numberOfLines={2}>
                      {[store.storeCity, store.storeFormat, formatDate(store.latestVisitDate)]
                        .filter(Boolean)
                        .join(" - ")}
                    </Text>
                    <Text style={styles.merchMetric}>
                      Merchandisers {formatNumber(store.merchandiserCount)}
                    </Text>
                  </View>
                ))}
              </View>
            ) : !isExecutionStoresLoading ? (
              <Text style={styles.bodyText}>No stores found for this period.</Text>
            ) : null}
          </View>
        ) : !isSearchMode ? (
          <View style={styles.statsGrid}>
            <Pressable style={styles.statCard} onPress={() => openExecutionStores("covered")}>
              <Text style={styles.statLabel}>Covered</Text>
              <Text style={styles.statValue}>{formatNumber(totalCoveredExecutions)}</Text>
              <Text style={styles.statDetail}>
                {formatNumber(coveredStores.length)} stores
              </Text>
            </Pressable>
            <Pressable style={styles.statCard} onPress={() => openExecutionStores("deviation")}>
              <Text style={styles.statLabel}>Deviations</Text>
              <Text style={styles.statValue}>{formatNumber(totalDeviationExecutions)}</Text>
              <Text style={styles.statDetail}>
                {formatNumber(deviationStores.length)} stores
              </Text>
            </Pressable>
          </View>
        ) : null}

        {!selectedMerchandiser && !selectedExecutionType ? (
          <View style={styles.panel}>
            <View style={styles.panelHeaderRow}>
              <View>
                <Text style={styles.panelTitle}>
                  {isSearchMode ? "Matching Merchandisers" : "Merchandiser Execution"}
                </Text>
                <Text style={styles.panelSubtitle}>
                  {isSearchMode
                    ? "Search mode hides the dashboard to focus on results"
                    : "Planned visits, covered stores, and deviations"}
                </Text>
              </View>
              <View style={styles.reportBadge}>
                <Text style={styles.reportBadgeText}>
                  {formatNumber(filteredMerchandisers.length)}
                </Text>
              </View>
            </View>

            {filteredMerchandisers.length > 0 ? (
              <View style={styles.merchList}>
                {filteredMerchandisers.map((merchandiser) => (
                  <Pressable
                    key={merchandiser.employeeCode}
                    onPress={() => openMerchandiserStores(merchandiser)}
                    style={styles.merchCard}
                  >
                    <View style={styles.merchHeaderRow}>
                      <View style={styles.merchTitleBlock}>
                        <Text style={styles.merchName} numberOfLines={1}>
                          {merchandiser.username || "Unknown merchandiser"}
                        </Text>
                        <Text style={styles.merchCode}>{merchandiser.employeeCode}</Text>
                      </View>
                    </View>

                    <View style={styles.merchMetricRow}>
                      <Text style={styles.merchMetric}>
                        Planned {formatNumber(merchandiser.plannedVisits)}
                      </Text>
                      <Text style={styles.merchMetric}>
                        Covered {formatNumber(merchandiser.storesCovered)}
                      </Text>
                      <Text style={styles.merchMetric}>
                        Dev {formatNumber(merchandiser.deviationVisits)}
                      </Text>
                    </View>

                    <Text style={styles.merchCities} numberOfLines={2}>
                      {(merchandiser.cities || []).join(", ") || "No city captured"}
                    </Text>
                  </Pressable>
                ))}
              </View>
            ) : (
              <Text style={styles.bodyText}>No merchandiser found for this filter.</Text>
            )}
          </View>
        ) : null}
      </ScrollView>
    </View>
  );
}
