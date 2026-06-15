import { useEffect, useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { getSupervisorMerchandiserExecution } from "../api/backendApi";
import { styles } from "../styles/appStyles";
import { formatNumber } from "../utils/formatters";

export function SupervisorProfileScreen({
  supervisor,
  overview,
  stores,
  onBack,
}) {
  const [assignedMerchandiserCount, setAssignedMerchandiserCount] = useState(
    supervisor?.assignedMerchandiserCount ?? null
  );
  const displayName = supervisor?.fullName || supervisor?.username || "Supervisor";
  const initials = getInitials(displayName);
  const assignedStoreCount = getFirstDefined(
    supervisor?.assignedStoreCount,
    overview?.storeActivity?.assignedStores,
    overview?.tableCounts?.stores,
    Array.isArray(stores) ? stores.length : undefined
  );
  const region = supervisor?.region || summarizeValues(stores, "region");
  const city = supervisor?.city || summarizeValues(stores, "city");
  const supervisorReference = supervisor?.supervisorCode || supervisor?.supervisorId;
  const assignedPerimeter = buildAssignedPerimeter(region, city);
  const assignedStoresLabel = formatOptionalNumber(assignedStoreCount);

  useEffect(() => {
    let isMounted = true;

    async function loadAssignedMerchandisers() {
      if (isPresent(supervisor?.assignedMerchandiserCount)) {
        setAssignedMerchandiserCount(supervisor.assignedMerchandiserCount);
        return;
      }

      if (!supervisor?.supervisorId) {
        setAssignedMerchandiserCount(null);
        return;
      }

      try {
        const result = await getSupervisorMerchandiserExecution(supervisor.supervisorId);

        if (isMounted) {
          setAssignedMerchandiserCount(countMerchandisers(result));
        }
      } catch (error) {
        if (isMounted) {
          setAssignedMerchandiserCount(null);
        }
      }
    }

    loadAssignedMerchandisers();

    return () => {
      isMounted = false;
    };
  }, [supervisor?.assignedMerchandiserCount, supervisor?.supervisorId]);

  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>
      <View style={styles.compactNavbar}>
        <Pressable style={styles.compactBackButton} onPress={onBack}>
          <Text style={styles.compactBackIcon}>{"‹"}</Text>
        </Pressable>
        <Text style={styles.compactNavTitle}>Supervisor Profile</Text>
        <View style={styles.compactNavSpacer} />
      </View>

      <View style={styles.profileHero}>
        <View style={styles.profileHeroAvatar}>
          <Text style={styles.profileHeroAvatarText}>{initials}</Text>
        </View>
        <View style={styles.profileHeroTextBlock}>
          <Text style={styles.profileHeroName} numberOfLines={2}>
            {displayName}
          </Text>
          <Text style={styles.profileHeroReference} numberOfLines={1}>
            {isPresent(supervisorReference) ? `Reference ${supervisorReference}` : "Reference --"}
          </Text>
          <Text style={styles.profileHeroEmail} numberOfLines={2}>
            {isPresent(supervisor?.email) ? supervisor.email : "--"}
          </Text>
        </View>
      </View>

      <Text style={styles.profileSectionTitle}>Quick stats</Text>
      <View style={styles.profileStatsGrid}>
        <ProfileStatCard label="Assigned stores" value={assignedStoresLabel} />
        <ProfileStatCard label="Assigned merch" value={formatOptionalNumber(assignedMerchandiserCount)} />
        <ProfileStatCard label="Region" value={region} />
        <ProfileStatCard label="City" value={city} />
      </View>

      <View style={styles.profilePerimeterCard}>
        <Text style={styles.profilePerimeterTitle}>Assigned perimeter</Text>
        <Text style={styles.profilePerimeterValue} numberOfLines={2}>
          {isPresent(assignedPerimeter) ? assignedPerimeter : "--"}
        </Text>
        {assignedStoresLabel !== "--" ? (
          <Text style={styles.profilePerimeterMeta}>{assignedStoresLabel} stores</Text>
        ) : null}
      </View>
    </ScrollView>
  );
}

function ProfileStatCard({ label, value }) {
  return (
    <View style={styles.profileStatCard}>
      <Text style={styles.profileStatLabel}>{label}</Text>
      <Text style={styles.profileStatValue} numberOfLines={2}>
        {isPresent(value) ? String(value) : "--"}
      </Text>
    </View>
  );
}

function formatOptionalNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }

  return formatNumber(value);
}

function summarizeValues(items, key) {
  if (!Array.isArray(items) || items.length === 0) {
    return null;
  }

  const values = Array.from(
    new Set(
      items
        .map((item) => item?.[key] || item?.[`store${capitalize(key)}`])
        .filter(isPresent)
    )
  );

  if (values.length === 0) {
    return null;
  }

  if (values.length <= 2) {
    return values.join(", ");
  }

  return `${values.slice(0, 2).join(", ")} +${values.length - 2}`;
}

function countMerchandisers(items) {
  if (!Array.isArray(items)) {
    return null;
  }

  const uniqueMerchandisers = new Set(
    items
      .map((item) => item?.employeeCode || item?.username)
      .filter(isPresent)
  );

  return uniqueMerchandisers.size || items.length;
}

function buildAssignedPerimeter(region, city) {
  return [city, region].filter(isPresent).join(" / ");
}

function getInitials(value) {
  return String(value)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part.charAt(0).toUpperCase())
    .join("") || "S";
}

function getFirstDefined(...values) {
  return values.find((value) => value !== null && value !== undefined);
}

function isPresent(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}

function capitalize(value) {
  return value ? value.charAt(0).toUpperCase() + value.slice(1) : "";
}
