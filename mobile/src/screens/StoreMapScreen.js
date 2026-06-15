import * as Location from "expo-location";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Animated,
  Dimensions,
  Easing,
  Keyboard,
  PanResponder,
  Pressable,
  ScrollView,
  Text,
  TextInput,
  View,
} from "react-native";
import MapView, { Marker } from "react-native-maps";
import {
  MOROCCO_REGION,
  STORE_FOCUS_DELTA,
} from "../constants/appConstants";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate } from "../utils/formatters";
import { sortBySearchScore } from "../utils/search";

const STORE_SHEET_EXPANDED_BOTTOM_OFFSET = 8;
const STORE_SHEET_COLLAPSED_HEIGHT = 48;
const STORE_SHEET_COLLAPSED_VISIBLE_HEIGHT = 28;
const STORE_SHEET_COLLAPSED_BOTTOM_OFFSET =
  STORE_SHEET_COLLAPSED_VISIBLE_HEIGHT - STORE_SHEET_COLLAPSED_HEIGHT;
const STORE_SHEET_DEFAULT_EXPANDED_HEIGHT = 360;
const STORE_SHEET_MAX_EXPANDED_HEIGHT = Math.min(
  Math.round(Dimensions.get("window").height * 0.55),
  460
);

export function StoreMapScreen({
  supervisorId,
  stores,
  isLoading,
  error,
  onSelectStore,
  selectedStore,
  onOpenStoreDetail,
  onOpenStoreExecution,
}) {
  const mapRef = useRef(null);
  const ignoreNextMapPressRef = useRef(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [userLocation, setUserLocation] = useState(null);
  const [locationError, setLocationError] = useState("");
  const [isStoreSheetExpanded, setIsStoreSheetExpanded] = useState(false);
  const [viewMode, setViewMode] = useState("map");
  const [storeStatusFilter, setStoreStatusFilter] = useState("all");
  const [sheetContentHeight, setSheetContentHeight] = useState(0);
  const sheetAnimation = useRef(new Animated.Value(0)).current;
  const sheetStartProgressRef = useRef(0);
  const expandedSheetHeight = sheetContentHeight
    ? Math.min(
        sheetContentHeight + STORE_SHEET_COLLAPSED_HEIGHT + 12,
        STORE_SHEET_MAX_EXPANDED_HEIGHT
      )
    : Math.min(
        STORE_SHEET_DEFAULT_EXPANDED_HEIGHT,
        STORE_SHEET_MAX_EXPANDED_HEIGHT
      );
  const sheetHeight = sheetAnimation.interpolate({
    inputRange: [0, 1],
    outputRange: [STORE_SHEET_COLLAPSED_HEIGHT, expandedSheetHeight],
  });
  const sheetBottom = sheetAnimation.interpolate({
    inputRange: [0, 1],
    outputRange: [
      STORE_SHEET_COLLAPSED_BOTTOM_OFFSET,
      STORE_SHEET_EXPANDED_BOTTOM_OFFSET,
    ],
  });
  const sheetContentOpacity = sheetAnimation.interpolate({
    inputRange: [0, 0.2, 1],
    outputRange: [0, 0, 1],
  });

  function animateStoreSheet(expanded) {
    Animated.timing(sheetAnimation, {
      toValue: expanded ? 1 : 0,
      duration: 500,
      easing: Easing.out(Easing.cubic),
      useNativeDriver: false,
    }).start();
  }

  const sheetPanResponder = useMemo(
    () =>
      PanResponder.create({
        onMoveShouldSetPanResponder: (_, gestureState) =>
          Math.abs(gestureState.dy) > 10 &&
          Math.abs(gestureState.dy) > Math.abs(gestureState.dx),
        onPanResponderGrant: () => {
          sheetAnimation.stopAnimation();
          sheetStartProgressRef.current = isStoreSheetExpanded ? 1 : 0;
        },
        onPanResponderMove: (_, gestureState) => {
          const dragDistance = expandedSheetHeight - STORE_SHEET_COLLAPSED_HEIGHT;
          const nextProgress =
            sheetStartProgressRef.current - gestureState.dy / dragDistance;

          sheetAnimation.setValue(Math.max(0, Math.min(1, nextProgress)));
        },
        onPanResponderRelease: (_, gestureState) => {
          const dragDistance = expandedSheetHeight - STORE_SHEET_COLLAPSED_HEIGHT;
          const nextProgress = Math.max(
            0,
            Math.min(1, sheetStartProgressRef.current - gestureState.dy / dragDistance)
          );

          if (gestureState.vy < -0.25 || gestureState.dy < -24) {
            if (isStoreSheetExpanded) {
              animateStoreSheet(true);
            } else {
              setIsStoreSheetExpanded(true);
            }
            return;
          }

          if (gestureState.vy > 0.25 || gestureState.dy > 24) {
            if (isStoreSheetExpanded) {
              setIsStoreSheetExpanded(false);
            } else {
              animateStoreSheet(false);
            }
            return;
          }

          const shouldExpand = nextProgress >= 0.5;
          if (shouldExpand === isStoreSheetExpanded) {
            animateStoreSheet(shouldExpand);
          } else {
            setIsStoreSheetExpanded(shouldExpand);
          }
        },
      }),
    [expandedSheetHeight, isStoreSheetExpanded, sheetAnimation]
  );

  useEffect(() => {
    animateStoreSheet(isStoreSheetExpanded);
  }, [isStoreSheetExpanded, sheetAnimation]);

  const validStores = useMemo(() => {
    const byStoreCode = new Map();
    stores
      .filter(
        (store) =>
          store.latitude !== null &&
          store.longitude !== null &&
          !Number.isNaN(Number(store.latitude)) &&
          !Number.isNaN(Number(store.longitude)) &&
          Number(store.latitude) !== 0 &&
          Number(store.longitude) !== 0
      )
      .forEach((store) => {
        if (!byStoreCode.has(store.storeCode)) {
          byStoreCode.set(store.storeCode, store);
        }
      });

    return Array.from(byStoreCode.values());
  }, [stores]);

  const filteredStores = useMemo(() => {
    const normalizedSearch = searchTerm.trim().toLowerCase();

    return stores
      .filter((store) => {
        if (storeStatusFilter === "deviation") {
          return hasStoreDeviation(store);
        }

        if (storeStatusFilter === "covered") {
          return !hasStoreDeviation(store);
        }

        return true;
      })
      .filter((store) => {
        if (!normalizedSearch) {
          return true;
        }

        return [
          store.storeCode,
          store.storeName,
          store.city,
          store.storeCity,
          store.region,
          store.storeRegion,
          store.storeFormat,
          store.username,
          store.merchandiserName,
          store.supervisorName,
        ]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(normalizedSearch));
      });
  }, [searchTerm, storeStatusFilter, stores]);

  const matchingStores = useMemo(() => {
    if (!searchTerm.trim() || selectedStore) {
      return [];
    }

    return sortBySearchScore(validStores, searchTerm, (store) => [
      store.storeCode,
      store.storeName,
      store.storeCity,
      store.storeFormat,
    ]).slice(0, 6);
  }, [searchTerm, selectedStore, validStores]);

  function getStoreCoordinate(store) {
    if (!store) {
      return null;
    }

    const latitude = Number(store.latitude);
    const longitude = Number(store.longitude);

    if (Number.isNaN(latitude) || Number.isNaN(longitude)) {
      return null;
    }

    return { latitude, longitude };
  }

  useEffect(() => {
    const coordinate = getStoreCoordinate(selectedStore);
    if (!coordinate) {
      return;
    }

    const timer = window.setTimeout(() => {
      mapRef.current?.animateToRegion(
        {
          ...coordinate,
          latitudeDelta: STORE_FOCUS_DELTA,
          longitudeDelta: STORE_FOCUS_DELTA,
        },
        450
      );
    }, 80);

    return () => window.clearTimeout(timer);
  }, [selectedStore]);

  useEffect(() => {
    let isMounted = true;

    async function loadUserLocation() {
      try {
        const permission = await Location.requestForegroundPermissionsAsync();

        if (permission.status !== "granted") {
          if (isMounted) {
            setLocationError("Location permission is off.");
          }
          return;
        }

        const position = await Location.getCurrentPositionAsync({
          accuracy: Location.Accuracy.Balanced,
        });

        if (isMounted) {
          setUserLocation({
            latitude: position.coords.latitude,
            longitude: position.coords.longitude,
          });
          setLocationError("");
        }
      } catch (locationLoadError) {
        if (isMounted) {
          setLocationError("Unable to get your location.");
        }
      }
    }

    loadUserLocation();

    return () => {
      isMounted = false;
    };
  }, []);

  function focusStore(store) {
    const coordinate = getStoreCoordinate(store);

    if (!coordinate) {
      return;
    }

    Keyboard.dismiss();
    onSelectStore(store);
    setIsStoreSheetExpanded(true);
    setSearchTerm(store.storeName || store.storeCode || "");
    mapRef.current?.animateToRegion(
      {
        ...coordinate,
        latitudeDelta: STORE_FOCUS_DELTA,
        longitudeDelta: STORE_FOCUS_DELTA,
      },
      450
    );
  }

  function resetMapView() {
    Keyboard.dismiss();
    setSearchTerm("");
    onSelectStore(null);
    setIsStoreSheetExpanded(false);
    mapRef.current?.animateToRegion(MOROCCO_REGION, 450);
  }

  function focusUserLocation() {
    if (!userLocation) {
      setLocationError("Location centering will be available after permission setup.");
      return;
    }

    Keyboard.dismiss();
    setLocationError("");
    mapRef.current?.animateToRegion(
      {
        ...userLocation,
        latitudeDelta: 0.018,
        longitudeDelta: 0.018,
      },
      450
    );
  }

  function handleSheetContentLayout(event) {
    const nextHeight = Math.ceil(event.nativeEvent.layout.height);

    setSheetContentHeight((currentHeight) =>
      Math.abs(currentHeight - nextHeight) > 2 ? nextHeight : currentHeight
    );
  }

  const selectedStoreDetails = selectedStore || {};
  const selectedStoreCity = selectedStoreDetails.city || selectedStoreDetails.storeCity;
  const selectedStoreRegion = selectedStoreDetails.region || selectedStoreDetails.storeRegion;
  const selectedStoreDate = selectedStoreDetails.latestVisitDate || selectedStoreDetails.visitDate;
  const selectedStoreMerchandiser =
    selectedStoreDetails.merchandiserName || selectedStoreDetails.username;
  const selectedStoreHasDeviation = hasStoreDeviation(selectedStoreDetails);
  const selectedStoreDeviationReason = selectedStoreHasDeviation
    ? selectedStoreDetails.deviationReason
    : null;
  const selectedStoreVisitId = getStoreVisitId(selectedStoreDetails);

  function openSelectedStoreExecution() {
    if (!selectedStore?.storeCode) {
      return;
    }

    if (selectedStoreVisitId) {
      onOpenStoreExecution?.(selectedStoreDetails);
      return;
    }

    onOpenStoreDetail?.(selectedStoreDetails);
  }

  function openStoreCard(store) {
    if (getStoreVisitId(store)) {
      onOpenStoreExecution?.(store);
      return;
    }

    onOpenStoreDetail?.(store);
  }

  return (
    <View style={styles.mapScreen}>
      {error ? <Text style={styles.errorText}>{error}</Text> : null}

      <View style={styles.mapSearchPanel}>
        {/* <View style={styles.storeMapNavbar}>
          <View style={styles.compactNavSpacer} />
          <Text style={styles.compactNavTitle}>Stores</Text>
          <View style={styles.compactNavSpacer} />
        </View> */}

        <View style={styles.mapSearchInputWrap}>
          {isLoading ? (
            <View style={styles.mapLoadingPill}>
              <ActivityIndicator color={colors.navy} size="small" />
            </View>
          ) : null}
          <TextInput
            value={searchTerm}
            onChangeText={(value) => {
              setSearchTerm(value);
              onSelectStore(null);
            }}
            placeholder="Search store name or code"
            placeholderTextColor={colors.muted}
            autoCapitalize="none"
            style={styles.mapSearchInput}
          />
          {searchTerm || selectedStore ? (
            <Pressable
              style={styles.mapSearchClear}
              onPress={resetMapView}
            >
              <Text style={styles.mapSearchClearText}>Clear</Text>
            </Pressable>
          ) : null}
        </View>

        {matchingStores.length > 0 ? (
          <View style={styles.mapSearchResults}>
            {matchingStores.map((store) => (
              <Pressable
                key={store.storeCode}
                style={styles.mapSearchResult}
                onPress={() => focusStore(store)}
              >
                <Text style={styles.mapSearchResultName} numberOfLines={1}>
                  {store.storeName}
                </Text>
                <Text style={styles.mapSearchResultCode} numberOfLines={1}>
                  {store.storeCode}
                  {store.storeCity ? ` - ${store.storeCity}` : ""}
                </Text>
              </Pressable>
            ))}
          </View>
        ) : null}

        <View style={styles.storeModeRow}>
          <Pressable
            style={[styles.storeModeButton, viewMode === "map" ? styles.storeModeButtonActive : null]}
            onPress={() => setViewMode("map")}
          >
            <Text style={[styles.storeModeText, viewMode === "map" ? styles.storeModeTextActive : null]}>
              Map
            </Text>
          </Pressable>
          <Pressable
            style={[styles.storeModeButton, viewMode === "list" ? styles.storeModeButtonActive : null]}
            onPress={() => setViewMode("list")}
          >
            <Text style={[styles.storeModeText, viewMode === "list" ? styles.storeModeTextActive : null]}>
              List
            </Text>
          </Pressable>
        </View>

        {viewMode === "list" ? (
          <View style={styles.storeStatusFilterRow}>
            {["all", "covered", "deviation"].map((status) => (
              <Pressable
                key={status}
                style={[
                  styles.storeStatusFilterChip,
                  storeStatusFilter === status ? styles.storeStatusFilterChipActive : null,
                ]}
                onPress={() => setStoreStatusFilter(status)}
              >
                <Text
                  style={[
                    styles.storeStatusFilterText,
                    storeStatusFilter === status ? styles.storeStatusFilterTextActive : null,
                  ]}
                >
                  {storeStatusFilterLabel(status)}
                </Text>
              </Pressable>
            ))}
          </View>
        ) : null}
      </View>

      {viewMode === "list" ? (
        <ScrollView contentContainerStyle={styles.storeListContent}>
          {isLoading ? (
            <View style={styles.inlineState}>
              <ActivityIndicator color={colors.navy} />
              <Text style={styles.bodyText}>Loading stores...</Text>
            </View>
          ) : null}

          {filteredStores.length > 0 ? (
            <View style={styles.storeCardList}>
              {filteredStores.map((store, index) => (
                <StoreListCard
                  key={`${store.storeCode}-${store.visitDate}-${index}`}
                  store={store}
                  onPress={() => openStoreCard(store)}
                />
              ))}
            </View>
          ) : !isLoading ? (
            <Text style={styles.emptyStateText}>No stores found for this filter.</Text>
          ) : null}
        </ScrollView>
      ) : (
      <MapView
        ref={mapRef}
        style={styles.map}
        initialRegion={MOROCCO_REGION}
        showsUserLocation={Boolean(userLocation)}
        showsMyLocationButton={false}
        onPress={() => {
          if (ignoreNextMapPressRef.current) {
            ignoreNextMapPressRef.current = false;
            return;
          }

          resetMapView();
        }}
      >
        {selectedStore ? (
          <Marker
            key={`selected-${selectedStore.storeCode}`}
            identifier={`selected-${selectedStore.storeCode}`}
            coordinate={getStoreCoordinate(selectedStore)}
            pinColor={colors.orange}
            title={selectedStore.storeName}
            description={selectedStore.storeCode}
            onPress={(event) => {
              ignoreNextMapPressRef.current = true;
              event.stopPropagation?.();
              focusStore(selectedStore);
            }}
          />
        ) : (
          validStores.map((store) => (
            <Marker
              key={`all-${store.storeCode}`}
              identifier={`all-${store.storeCode}`}
              coordinate={getStoreCoordinate(store)}
              pinColor={colors.navy}
              title={store.storeName}
              description={store.storeCode}
              onPress={(event) => {
                ignoreNextMapPressRef.current = true;
                event.stopPropagation?.();
                focusStore(store);
              }}
            />
          ))
        )}
      </MapView>
      )}

      {viewMode === "map" ? (
      <View
        style={[
          styles.locationControls,
          selectedStore
            ? {
                bottom:
                  (isStoreSheetExpanded
                    ? STORE_SHEET_EXPANDED_BOTTOM_OFFSET + expandedSheetHeight
                    : STORE_SHEET_COLLAPSED_VISIBLE_HEIGHT) +
                  12,
              }
            : null,
        ]}
      >
        {userLocation ? (
          <Pressable
            style={styles.locationButton}
            onPress={focusUserLocation}
            accessibilityRole="button"
            accessibilityLabel="Center on my location"
            accessibilityHint="Moves the map to your current location"
            hitSlop={8}
          >
            <Text style={styles.locationButtonText}>{"\u2316"}</Text>
          </Pressable>
        ) : null}
        {locationError ? (
          <View style={styles.locationMessage}>
            <Text style={styles.locationMessageText}>{locationError}</Text>
          </View>
        ) : null}
      </View>
      ) : null}

      {viewMode === "map" && selectedStore ? (
        <Animated.View
          style={[
            styles.storeSheet,
            { bottom: sheetBottom, height: sheetHeight },
          ]}
        >
          <View style={styles.storeSheetDragArea} {...sheetPanResponder.panHandlers}>
            <Pressable
              style={styles.sheetHandleButton}
              onPress={() => setIsStoreSheetExpanded((current) => !current)}
            >
              <View style={styles.sheetHandle} />
            </Pressable>
          </View>

          <Animated.View
            pointerEvents={isStoreSheetExpanded ? "auto" : "none"}
            style={[styles.storeSheetContentWrap, { opacity: sheetContentOpacity }]}
          >
            <ScrollView
              showsVerticalScrollIndicator={false}
              contentContainerStyle={styles.storeSheetScrollContent}
            >
              <View style={styles.storeSheetMeasuredContent} onLayout={handleSheetContentLayout}>
                <View style={styles.storeSheetHeader}>
                  <View style={styles.storeSheetTitleBlock}>
                    <Text
                      style={styles.sheetTitle}
                      numberOfLines={2}
                      adjustsFontSizeToFit
                      minimumFontScale={0.86}
                    >
                      {selectedStore.storeName}
                    </Text>
                    <Text style={styles.storeCodeText}>{selectedStore.storeCode}</Text>
                    <Text style={styles.storeSheetMetaText} numberOfLines={2}>
                      {[selectedStoreDetails.storeFormat, selectedStoreCity, selectedStoreRegion]
                        .filter(Boolean)
                        .join(" - ") || "--"}
                    </Text>
                  </View>
                </View>

                <View style={styles.storeSheetSummary}>
                  <View style={styles.storeSheetInfoStack}>
                    <StoreSheetInfoRow
                      label="Channel"
                      value={formatChannelLabel(selectedStoreDetails.storeFormat)}
                    />
                    <StoreSheetInfoRow
                      label="Status"
                      value={selectedStoreHasDeviation ? "Deviation" : "Covered"}
                      tone={selectedStoreHasDeviation ? "warning" : "success"}
                    />
                    <StoreSheetInfoRow
                      label="Monthly visits"
                      value={formatCount(selectedStoreDetails.monthlyVisitCount)}
                    />
                    <StoreSheetInfoRow
                      label="Latest visit"
                      value={formatSafeDate(selectedStoreDate)}
                    />
                    <StoreSheetInfoRow
                      label="Merchandiser"
                      value={selectedStoreMerchandiser || "--"}
                    />
                  </View>
                  {selectedStoreDeviationReason ? (
                    <Text style={styles.storeSheetReasonText} numberOfLines={2}>
                      Reason: {selectedStoreDeviationReason}
                    </Text>
                  ) : null}
                  <Pressable
                    style={styles.storeSheetActionButton}
                    onPress={openSelectedStoreExecution}
                  >
                    <Text style={styles.storeSheetActionText}>View details</Text>
                  </Pressable>
                </View>
              </View>
            </ScrollView>
          </Animated.View>
        </Animated.View>
      ) : null}
    </View>
  );
}

function StoreSheetInfoRow({ label, tone, value }) {
  return (
    <View style={styles.storeSheetInfoRow}>
      <Text style={styles.storeSheetInfoLabel}>{label}</Text>
      {tone ? (
        <StoreBadge label={value} tone={tone} />
      ) : (
        <Text style={styles.storeSheetInfoValue} numberOfLines={1}>
          {value}
        </Text>
      )}
    </View>
  );
}

function StoreListCard({ onPress, store }) {
  const city = store.city || store.storeCity;
  const region = store.region || store.storeRegion;
  const visitDate = store.latestVisitDate || store.visitDate;
  const merchandiser = store.merchandiserName || store.username;
  const supervisor = store.supervisorName;
  const hasDeviation = hasStoreDeviation(store);
  const hasVisit = Boolean(getStoreVisitId(store));

  return (
    <Pressable style={styles.storeListCard} onPress={onPress}>
      <Text style={styles.storeListCardTitle} numberOfLines={1}>
        {store.storeName || "Unknown store"}
      </Text>
      <Text style={styles.storeListCardSubtitle} numberOfLines={1}>
        {[store.storeCode, store.storeFormat].filter(Boolean).join(" - ") || "--"}
      </Text>
      <Text style={styles.storeListCardMeta} numberOfLines={2}>
        {[city, region, formatSafeDate(visitDate)].filter(isPresent).join(" - ") || "--"}
      </Text>
      <View style={styles.storeListChipRow}>
        <StoreBadge label={formatChannelLabel(store.storeFormat)} />
        <StoreBadge
          label={hasDeviation ? "Deviation" : "Covered"}
          tone={hasDeviation ? "warning" : "success"}
        />
        <StoreBadge label={`Monthly visits: ${formatCount(store.monthlyVisitCount)}`} />
      </View>
      <Text style={styles.storeListAssignmentText} numberOfLines={1}>
        {[merchandiser, supervisor].filter(isPresent).join(" - ") || "--"}
      </Text>
      <Text style={[styles.storeListActionText, !hasVisit ? styles.storeListActionTextMuted : null]}>
        {hasVisit ? "View details" : "No execution available"}
      </Text>
    </Pressable>
  );
}

function StoreBadge({ label, tone }) {
  return (
    <View
      style={[
        styles.storeBadge,
        tone === "success" ? styles.storeBadgeSuccess : null,
        tone === "warning" ? styles.storeBadgeWarning : null,
      ]}
    >
      <Text
        style={[
          styles.storeBadgeText,
          tone === "success" ? styles.storeBadgeTextSuccess : null,
          tone === "warning" ? styles.storeBadgeTextWarning : null,
        ]}
      >
        {label}
      </Text>
    </View>
  );
}

function formatSafeDate(value) {
  if (!value) {
    return "--";
  }

  return formatDate(value);
}

function formatCount(value) {
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) {
    return "0";
  }

  return String(numericValue);
}

function formatChannelLabel(format) {
  if (!format) {
    return "--";
  }

  return String(format).trim().toUpperCase() === "GROCERY" ? "GT" : "MT";
}

function getStoreVisitId(store) {
  return store?.latestVisitId || store?.visitId;
}

function hasStoreDeviation(store) {
  return Boolean(store?.hasDeviation ?? store?.deviation);
}

function storeStatusFilterLabel(status) {
  if (status === "covered") return "Covered";
  if (status === "deviation") return "Deviation";
  return "All";
}

function isPresent(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}
