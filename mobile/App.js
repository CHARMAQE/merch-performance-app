import { StatusBar } from "expo-status-bar";
import { useEffect, useState } from "react";
import { SafeAreaView, View } from "react-native";
import {
  getSupervisorDashboardOverview,
  getSupervisorStores,
} from "./src/api/backendApi";
import { TabButton } from "./src/components/TabButton";
import { REPORT_YEAR } from "./src/constants/appConstants";
import { DashboardScreen } from "./src/screens/DashboardScreen";
import { LoginScreen } from "./src/screens/LoginScreen";
import { StoreMapScreen } from "./src/screens/StoreMapScreen";
import { styles } from "./src/styles/appStyles";

export default function App() {
  const [supervisor, setSupervisor] = useState(null);
  const [activeScreen, setActiveScreen] = useState("home");
  const [selectedMonth, setSelectedMonth] = useState(4);
  const [selectedDay, setSelectedDay] = useState(null);
  const [overview, setOverview] = useState(null);
  const [stores, setStores] = useState([]);
  const [selectedStore, setSelectedStore] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  function handleLogout() {
    setSupervisor(null);
    setActiveScreen("home");
    setOverview(null);
    setStores([]);
    setSelectedStore(null);
    setError("");
  }

  async function loadData() {
    try {
      setIsLoading(true);
      setError("");

      const [overviewResult, storesResult] = await Promise.allSettled([
        getSupervisorDashboardOverview(supervisor.supervisorId, {
          year: REPORT_YEAR,
          month: selectedMonth,
          day: selectedDay,
        }),
        getSupervisorStores(supervisor.supervisorId),
      ]);

      if (overviewResult.status === "fulfilled") {
        setOverview(overviewResult.value);
      }

      if (storesResult.status === "fulfilled") {
        setStores(Array.isArray(storesResult.value) ? storesResult.value : []);
      }

      const messages = [];
      if (overviewResult.status === "rejected") {
        messages.push("Dashboard data failed");
      }
      if (storesResult.status === "rejected") {
        messages.push("Map stores failed");
      }
      if (messages.length > 0) {
        setError(`${messages.join(". ")}. Check backend and Wi-Fi.`);
      }
    } catch (loadError) {
      setError(loadError.message || "Unable to load app data.");
    } finally {
      setIsLoading(false);
    }
  }

  useEffect(() => {
    if (supervisor) {
      loadData();
    }
  }, [supervisor]);

  if (!supervisor) {
    return (
      <>
        <LoginScreen onLogin={setSupervisor} />
        <StatusBar style="dark" />
      </>
    );
  }

  return (
    <SafeAreaView style={styles.appShell}>
      <View style={styles.content}>
        {activeScreen === "home" ? (
          <DashboardScreen
            supervisor={supervisor}
            username={supervisor.fullName || supervisor.username}
            overview={overview}
            isLoading={isLoading}
            error={error}
            selectedMonth={selectedMonth}
            selectedDay={selectedDay}
            onMonthChange={setSelectedMonth}
            onDayChange={setSelectedDay}
            onRefresh={loadData}
            onLogout={handleLogout}
          />
        ) : (
          <StoreMapScreen
            supervisorId={supervisor.supervisorId}
            stores={stores}
            isLoading={isLoading}
            error={error}
            selectedStore={selectedStore}
            onSelectStore={setSelectedStore}
          />
        )}
      </View>

      <View style={styles.tabs}>
        <TabButton
          label="Dashboard"
          isActive={activeScreen === "home"}
          onPress={() => setActiveScreen("home")}
        />
        <TabButton
          label="Map"
          isActive={activeScreen === "map"}
          onPress={() => setActiveScreen("map")}
        />
      </View>
      <StatusBar style="dark" />
    </SafeAreaView>
  );
}
