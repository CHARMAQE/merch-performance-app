import { useState } from "react";
import { ActivityIndicator, Pressable, SafeAreaView, Text, TextInput, View } from "react-native";
import { loginSupervisor } from "../api/backendApi";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";

export function LoginScreen({ onLogin }) {
  const [username, setUsername] = useState("casa_sup");
  const [password, setPassword] = useState("1234");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleLogin() {
    if (!username.trim() || !password.trim()) {
      setError("Please enter username and password.");
      return;
    }

    try {
      setIsLoading(true);
      setError("");
      const supervisor = await loginSupervisor(username.trim(), password.trim());
      onLogin(supervisor);
    } catch (loginError) {
      setError("Invalid login or backend is unreachable.");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <SafeAreaView style={styles.screen}>
      <View style={styles.loginPanel}>
        <Text style={styles.eyebrow}>Merch Performance</Text>
        <Text style={styles.title}>Supervisor login</Text>
        <Text style={styles.bodyText}>
          Sign in to review your assigned stores and store information.
        </Text>

        <Text style={styles.label}>Username</Text>
        <TextInput
          value={username}
          onChangeText={setUsername}
          placeholder="Enter username"
          autoCapitalize="none"
          style={styles.input}
        />

        <Text style={styles.label}>Password</Text>
        <TextInput
          value={password}
          onChangeText={setPassword}
          placeholder="Enter password"
          secureTextEntry
          style={styles.input}
        />

        {error ? <Text style={styles.errorText}>{error}</Text> : null}

        <Pressable
          style={[styles.primaryButton, isLoading ? styles.disabledButton : null]}
          onPress={handleLogin}
          disabled={isLoading}
        >
          {isLoading ? (
            <ActivityIndicator color={colors.white} />
          ) : (
            <Text style={styles.primaryButtonText}>Login</Text>
          )}
        </Pressable>
      </View>
    </SafeAreaView>
  );
}
