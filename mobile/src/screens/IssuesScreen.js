import { ActivityIndicator, Pressable, ScrollView, Text, View } from "react-native";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate } from "../utils/formatters";

export function IssuesScreen({ error, issues = [], isLoading, onOpenStore }) {
  return (
    <ScrollView contentContainerStyle={styles.scrollContent}>
      <View>
        <Text style={styles.eyebrow}>SITUATIONS TO REVIEW</Text>
        <Text style={styles.title}>Follow-up</Text>
        <Text style={styles.bodyText}>
          Situations to review: non visited stores, deviations, rejected visits, and GPS checks.
        </Text>
      </View>

      {isLoading ? (
        <View style={styles.inlineState}>
          <ActivityIndicator color={colors.navy} />
          <Text style={styles.bodyText}>Loading follow-up situations...</Text>
        </View>
      ) : null}

      {error ? <Text style={styles.errorText}>{error}</Text> : null}

      {issues.length > 0 ? (
        <View style={styles.issueList}>
          {issues.map((issue, index) => (
            <Pressable
              key={`${issue.type}-${issue.storeCode}-${issue.visitDate}-${index}`}
              style={styles.issueCard}
              onPress={() => onOpenStore?.(issue)}
            >
              <View style={styles.storeRow}>
                <View style={styles.merchTitleBlock}>
                  <Text style={styles.merchName} numberOfLines={1}>
                    {issue.storeName || "Unknown store"}
                  </Text>
                  <Text style={styles.merchCode}>{issue.storeCode}</Text>
                </View>
                <View style={[styles.issueTypeBadge, issueTypeStyle(issue.type)]}>
                  <Text style={styles.statusBadgeText}>{issueTypeLabel(issue.type)}</Text>
                </View>
              </View>

              <View style={styles.merchMetricRow}>
                <View style={[styles.statusBadge, severityStyle(issue.severity)]}>
                  <Text style={styles.statusBadgeText}>{issue.severity || "LOW"}</Text>
                </View>
                <Text style={styles.merchMetric}>{formatDate(issue.visitDate)}</Text>
              </View>

              <Text style={styles.merchCities} numberOfLines={2}>
                {[issue.city, issue.region, issue.username, issue.supervisorName]
                  .filter(Boolean)
                  .join(" - ")}
              </Text>
              {issue.reason ? (
                <Text style={styles.merchReasons} numberOfLines={2}>
                  {issue.reason}
                </Text>
              ) : null}
            </Pressable>
          ))}
        </View>
      ) : !isLoading ? (
        <Text style={styles.emptyStateText}>No follow-up situations found for this filter.</Text>
      ) : null}
    </ScrollView>
  );
}

function issueTypeStyle(type) {
  if (type === "NON_VISITED") return styles.statusBadgeNonVisited;
  if (type === "DEVIATION") return styles.statusBadgeDeviation;
  if (type === "REJECTED") return styles.statusBadgeRejected;
  if (type === "GPS_DISTANCE") return styles.statusBadgeGps;
  if (type === "LOW_TASK_COMPLETION") return styles.statusBadgeNonVisited;
  return styles.statusBadgeMuted;
}

function issueTypeLabel(type) {
  if (type === "NON_VISITED") return "Non Visited";
  if (type === "DEVIATION") return "Deviation";
  if (type === "REJECTED") return "Rejected";
  if (type === "GPS_DISTANCE") return "GPS Distance";
  if (type === "LOW_TASK_COMPLETION") return "Low Task Completion";
  return type || "Follow-up";
}

function severityStyle(severity) {
  if (severity === "HIGH") return styles.statusBadgeDeviation;
  if (severity === "MEDIUM") return styles.statusBadgeNonVisited;
  return styles.statusBadgeMuted;
}
