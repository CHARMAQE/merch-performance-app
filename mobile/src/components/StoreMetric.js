import { Text, View } from "react-native";
import { styles } from "../styles/appStyles";

export function StoreMetric({ label, value }) {
  return (
    <View style={styles.storeMetric}>
      <Text style={styles.storeMetricLabel}>{label}</Text>
      <Text style={styles.storeMetricValue}>{value}</Text>
    </View>
  );
}
