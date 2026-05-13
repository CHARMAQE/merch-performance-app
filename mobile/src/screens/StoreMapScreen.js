import * as Location from "expo-location";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Animated,
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
import { getSupervisorStoreDetails } from "../api/backendApi";
import { StoreMetric } from "../components/StoreMetric";
import {
  MOROCCO_REGION,
  STORE_FOCUS_DELTA,
  STORE_SHEET_COMPACT_HEIGHT,
  STORE_SHEET_EXPANDED_HEIGHT,
  STORE_SHEET_LONG_NAME_HEIGHT,
} from "../constants/appConstants";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate, formatNumber, formatPercentage } from "../utils/formatters";
import { sortBySearchScore } from "../utils/search";

export function StoreMapScreen({
  supervisorId,
  stores,
  isLoading,
  error,
  onSelectStore,
  selectedStore,
}) {
  const mapRef = useRef(null);
  const ignoreNextMapPressRef = useRef(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [userLocation, setUserLocation] = useState(null);
  const [locationError, setLocationError] = useState("");
  const [storeDetails, setStoreDetails] = useState(null);
  const [isStoreDetailsLoading, setIsStoreDetailsLoading] = useState(false);
  const [storeDetailsError, setStoreDetailsError] = useState("");
  const [isStoreSheetExpanded, setIsStoreSheetExpanded] = useState(false);
  const sheetAnimation = useRef(new Animated.Value(0)).current;
  const sheetStartProgressRef = useRef(0);
  const selectedStoreName = selectedStore?.storeName || "";
  const compactSheetHeight =
    selectedStoreName.length > 18
      ? STORE_SHEET_LONG_NAME_HEIGHT
      : STORE_SHEET_COMPACT_HEIGHT;
  const sheetHeight = sheetAnimation.interpolate({
    inputRange: [0, 1],
    outputRange: [compactSheetHeight, STORE_SHEET_EXPANDED_HEIGHT],
  });
  const sheetDetailsOpacity = sheetAnimation.interpolate({
    inputRange: [0, 1],
    outputRange: [0, 1],
  });
  const sheetDetailsHeight = sheetAnimation.interpolate({
    inputRange: [0, 1],
    outputRange: [0, 350],
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
          const dragDistance = STORE_SHEET_EXPANDED_HEIGHT - compactSheetHeight;
          const nextProgress =
            sheetStartProgressRef.current - gestureState.dy / dragDistance;

          sheetAnimation.setValue(Math.max(0, Math.min(1, nextProgress)));
        },
        onPanResponderRelease: (_, gestureState) => {
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

          animateStoreSheet(isStoreSheetExpanded);
        },
      }),
    [compactSheetHeight, isStoreSheetExpanded, sheetAnimation]
  );

  useEffect(() => {
    animateStoreSheet(isStoreSheetExpanded);
  }, [isStoreSheetExpanded, sheetAnimation]);

  const validStores = useMemo(
    () =>
      stores.filter(
        (store) =>
          store.latitude !== null &&
          store.longitude !== null &&
          !Number.isNaN(Number(store.latitude)) &&
          !Number.isNaN(Number(store.longitude))
      ),
    [stores]
  );

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

    async function loadStoreDetails() {
      if (!selectedStore?.storeCode) {
        setStoreDetails(null);
        setStoreDetailsError("");
        return;
      }

      if (!supervisorId) {
        setStoreDetails(null);
        setStoreDetailsError("Supervisor login is missing.");
        return;
      }

      try {
        setIsStoreDetailsLoading(true);
        setStoreDetailsError("");
        const details = await getSupervisorStoreDetails(
          supervisorId,
          selectedStore.storeCode
        );

        if (isMounted) {
          setStoreDetails(details);
        }
      } catch (detailLoadError) {
        if (isMounted) {
          setStoreDetails(null);
          setStoreDetailsError("Restart backend to load details.");
        }
      } finally {
        if (isMounted) {
          setIsStoreDetailsLoading(false);
        }
      }
    }

    loadStoreDetails();

    return () => {
      isMounted = false;
    };
  }, [selectedStore, supervisorId]);

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
    setIsStoreSheetExpanded(false);
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
      return;
    }

    Keyboard.dismiss();
    mapRef.current?.animateToRegion(
      {
        ...userLocation,
        latitudeDelta: 0.018,
        longitudeDelta: 0.018,
      },
      450
    );
  }

  const merchandiserLabel = storeDetails?.merchandiserName
    ? `${storeDetails.merchandiserName}${storeDetails?.merchandiserUserId ? ` (${storeDetails.merchandiserUserId})` : ""}`
    : "N/A";

  return (
    <View style={styles.mapScreen}>
      {error ? <Text style={styles.errorText}>{error}</Text> : null}

      <View style={styles.mapSearchPanel}>
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
      </View>

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

      <View
        style={[
          styles.locationControls,
          selectedStore
            ? {
                bottom:
                  (isStoreSheetExpanded
                    ? STORE_SHEET_EXPANDED_HEIGHT
                    : compactSheetHeight) + 12,
              }
            : null,
        ]}
      >
        {userLocation ? (
          <Pressable style={styles.locationButton} onPress={focusUserLocation}>
            <Text style={styles.locationButtonText}>O</Text>
          </Pressable>
        ) : null}
        {locationError ? (
          <View style={styles.locationMessage}>
            <Text style={styles.locationMessageText}>{locationError}</Text>
          </View>
        ) : null}
      </View>

      {selectedStore ? (
        <Animated.View
          style={[
            styles.storeSheet,
            isStoreSheetExpanded ? styles.storeSheetExpanded : null,
            { height: sheetHeight },
          ]}
        >
          <View style={styles.storeSheetDragArea} {...sheetPanResponder.panHandlers}>
            <Pressable
              style={styles.sheetHandleButton}
              onPress={() => setIsStoreSheetExpanded((current) => !current)}
            >
              <View style={styles.sheetHandle} />
            </Pressable>
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
              </View>
              <View
                style={[
                  styles.statusPill,
                  Number(storeDetails?.coverageRatePercentage) >= 90
                    ? styles.statusPillGood
                    : Number(storeDetails?.coverageRatePercentage) > 0
                      ? styles.statusPillWarning
                      : styles.statusPillMuted,
                ]}
              >
                <Text style={styles.statusPillLabel}>Taux couverture</Text>
                <Text style={styles.statusPillText}>
                  {storeDetailsError
                    ? "Unavailable"
                    : isStoreDetailsLoading
                      ? "Loading"
                      : formatPercentage(storeDetails?.coverageRatePercentage)}
                </Text>
              </View>
            </View>
          </View>

          <View style={styles.osaBlock}>
            <View>
              <Text style={styles.storeMetricLabel}>OSA</Text>
              <Text style={styles.osaValue}>
                {isStoreDetailsLoading
                  ? "..."
                  : formatPercentage(storeDetails?.osaPercentage)}
              </Text>
            </View>
            <View style={styles.osaTrack}>
              <View
                style={[
                  styles.osaFill,
                  {
                    width: `${Math.max(
                      0,
                      Math.min(100, Number(storeDetails?.osaPercentage) || 0)
                    )}%`,
                  },
                ]}
              />
            </View>
          </View>

          <Animated.View
            pointerEvents={isStoreSheetExpanded ? "auto" : "none"}
            style={[
              styles.storeDetailsPanel,
              { height: sheetDetailsHeight, opacity: sheetDetailsOpacity },
            ]}
          >
            <ScrollView
              showsVerticalScrollIndicator={false}
              contentContainerStyle={styles.storeDetailsScrollContent}
            >
              {storeDetailsError ? (
                <Text style={styles.storeDetailsError}>{storeDetailsError}</Text>
              ) : null}

              <View style={styles.storeMetricGrid}>
                <Text style={styles.storeDetailsSectionTitle}>Visit information</Text>
                <StoreMetric label="City" value={selectedStore.storeCity || "N/A"} />
                <StoreMetric
                  label="Monthly visits"
                  value={formatNumber(storeDetails?.monthlyVisitCount)}
                />
                <StoreMetric
                  label="Last visit"
                  value={formatDate(storeDetails?.lastVisitDate)}
                />
                <StoreMetric
                  label="Merchandiser"
                  value={merchandiserLabel}
                />
                <StoreMetric
                  label="Deviation"
                  value={formatPercentage(storeDetails?.deviationPercentage)}
                />
              </View>
            </ScrollView>
          </Animated.View>
        </Animated.View>
      ) : null}
    </View>
  );
}
