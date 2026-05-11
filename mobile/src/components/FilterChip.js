import { Pressable, Text } from "react-native";
import { styles } from "../styles/appStyles";

export function FilterChip({ label, isActive, onPress }) {
  return (
    <Pressable
      style={[styles.filterChip, isActive ? styles.filterChipActive : null]}
      onPress={onPress}
    >
      <Text
        style={[
          styles.filterChipText,
          isActive ? styles.filterChipTextActive : null,
        ]}
      >
        {label}
      </Text>
    </Pressable>
  );
}
