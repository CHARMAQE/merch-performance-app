import { useState } from "react";
import {
  ActivityIndicator,
  Image,
  Pressable,
  SafeAreaView,
  Text,
  TextInput,
  View,
} from "react-native";
import { loginSupervisor } from "../api/backendApi";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";

const smollanLogo = require("../../assets/smollan.png");
const unileverLogo = require("../../assets/unilever.png");

export function LoginScreen({ onLogin }) {
  const [email, setEmail] = useState("yassine.elamrani@unilever.test");
  const [password, setPassword] = useState("1234");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleLogin() {
    if (!email.trim() || !password.trim()) {
      setError("Please enter email and password.");
      return;
    }

    const loginEmail = email.trim().toLowerCase();
    const loginPassword = password;

    try {
      setIsLoading(true);
      setError("");
      const supervisor = await loginSupervisor(loginEmail, loginPassword);
      onLogin(supervisor);
    } catch (loginError) {
      setError(getLoginErrorMessage(loginError));
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <SafeAreaView style={styles.loginScreen}>
      <View style={styles.loginPanel}>
        <View style={styles.loginLogoRow}>
          <View style={styles.loginLogoCard}>
            <Image source={smollanLogo} style={styles.smollanLogo} resizeMode="contain" />
          </View>
          <View style={styles.loginLogoCard}>
            <Image source={unileverLogo} style={styles.unileverLogo} resizeMode="contain" />
          </View>
        </View>

        <Text style={styles.eyebrow}>Assigned store access</Text>
        <Text style={styles.title}>Mobile supervision</Text>
        <Text style={styles.bodyText}>
          Sign in with your Unilever email to monitor execution, coverage,
          merchandisers, and assigned stores.
        </Text>

        <Text style={styles.label}>Email</Text>
        <TextInput
          value={email}
          onChangeText={setEmail}
          placeholder="name@unilever.test"
          autoCapitalize="none"
          autoCorrect={false}
          keyboardType="email-address"
          textContentType="emailAddress"
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

function getLoginErrorMessage(loginError) {
  if (loginError?.code === "TIMEOUT" || loginError?.code === "NETWORK") {
    return "Backend unreachable. Check Wi-Fi, PC IP, and that Spring Boot is running.";
  }

  if (loginError?.status === 401) {
    return "401 invalid credentials. Check the email and password.";
  }

  if (loginError?.status === 400) {
    return "400 bad request. The login payload may not match the backend contract.";
  }

  if (loginError?.status >= 500) {
    return "Server error. Check the Spring Boot console.";
  }

  if (loginError?.status) {
    return `Login failed with HTTP ${loginError.status}. Check Expo logs.`;
  }

  return "Login failed. Check Expo logs for details.";
}
