import { Pressable, Text } from "react-native";
import { styles } from "../styles/appStyles";

export function TabButton({ label, isActive, onPress }) {
  return (
    <Pressable
      style={[styles.tabButton, isActive ? styles.tabButtonActive : null]}
      onPress={onPress}
    >
      <Text
        style={[
          styles.tabButtonText,
          isActive ? styles.tabButtonTextActive : null,
        ]}
      >
        {label}
      </Text>
    </Pressable>
  );
}
