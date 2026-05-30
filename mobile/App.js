import { StatusBar } from "expo-status-bar";
import { useEffect, useState } from "react";
import { SafeAreaView, View } from "react-native";
import {
  getSupervisorDashboardOverview,
  getSupervisorIssues,
  getSupervisorMerchandiserExecution,
  getSupervisorStores,
} from "./src/api/backendApi";
import { TabButton } from "./src/components/TabButton";
import { DEFAULT_REPORT_MONTH, REPORT_YEAR } from "./src/constants/appConstants";
import { DashboardScreen } from "./src/screens/DashboardScreen";
import { LoginScreen } from "./src/screens/LoginScreen";
import { IssuesScreen } from "./src/screens/IssuesScreen";
import { MerchandiserExecutionScreen } from "./src/screens/MerchandiserExecutionScreen";
import { StoreDetailScreen } from "./src/screens/StoreDetailScreen";
import { StoreMapScreen } from "./src/screens/StoreMapScreen";
import { styles } from "./src/styles/appStyles";

export default function App() {
  const [supervisor, setSupervisor] = useState(null);
  const [activeScreen, setActiveScreen] = useState("home");
  const [selectedMonth, setSelectedMonth] = useState(DEFAULT_REPORT_MONTH);
  const [selectedDay, setSelectedDay] = useState(null);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [merchStoreFormatGroup, setMerchStoreFormatGroup] = useState("ALL");
  const [overview, setOverview] = useState(null);
  const [merchandisers, setMerchandisers] = useState([]);
  const [stores, setStores] = useState([]);
  const [issues, setIssues] = useState([]);
  const [selectedStore, setSelectedStore] = useState(null);
  const [selectedStoreDetail, setSelectedStoreDetail] = useState(null);
  const [storeDetailBackScreen, setStoreDetailBackScreen] = useState("map");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  function handleLogout() {
    setSupervisor(null);
    setActiveScreen("home");
    setOverview(null);
    setMerchandisers([]);
    setStores([]);
    setIssues([]);
    setSelectedStore(null);
    setSelectedStoreDetail(null);
    setError("");
  }

  async function loadData() {
    try {
      setIsLoading(true);
      setError("");

      const filters = {
        ...(startDate || endDate
          ? {
              startDate: startDate || undefined,
              endDate: endDate || undefined,
            }
          : {
              year: REPORT_YEAR,
              month: selectedMonth,
              day: selectedDay,
            }),
      };

      const [overviewResult, merchandisersResult, storesResult, issuesResult] = await Promise.allSettled([
        getSupervisorDashboardOverview(supervisor.supervisorId, {
          ...filters,
        }),
        getSupervisorMerchandiserExecution(supervisor.supervisorId, {
          ...filters,
          storeFormatGroup: merchStoreFormatGroup,
        }),
        getSupervisorStores(supervisor.supervisorId, {
          ...filters,
        }),
        getSupervisorIssues(supervisor.supervisorId, {
          ...filters,
        }),
      ]);

      if (overviewResult.status === "fulfilled") {
        setOverview(overviewResult.value);
      }

      if (merchandisersResult.status === "fulfilled") {
        setMerchandisers(
          Array.isArray(merchandisersResult.value) ? merchandisersResult.value : []
        );
      }

      if (storesResult.status === "fulfilled") {
        setStores(Array.isArray(storesResult.value) ? storesResult.value : []);
      }

      if (issuesResult.status === "fulfilled") {
        setIssues(Array.isArray(issuesResult.value) ? issuesResult.value : []);
      }

      const messages = [];
      if (overviewResult.status === "rejected") {
        messages.push("Dashboard data failed");
      }
      if (merchandisersResult.status === "rejected") {
        messages.push("Merchandiser data failed");
      }
      if (storesResult.status === "rejected") {
        messages.push("Stores failed");
      }
      if (issuesResult.status === "rejected") {
        messages.push("Follow-up failed");
      }
      if (messages.length > 0) {
        setError("Unable to load data. Please check backend connection.");
      }
    } catch (loadError) {
      setError("Unable to load data. Please check backend connection.");
    } finally {
      setIsLoading(false);
    }
  }

  async function loadMerchandisers(nextStoreFormatGroup = merchStoreFormatGroup) {
    if (!supervisor) {
      return;
    }

    try {
      setIsLoading(true);
      setError("");

      const filters = {
        ...(startDate || endDate
          ? {
              startDate: startDate || undefined,
              endDate: endDate || undefined,
            }
          : {
              year: REPORT_YEAR,
              month: selectedMonth,
              day: selectedDay,
            }),
        storeFormatGroup: nextStoreFormatGroup,
      };

      const result = await getSupervisorMerchandiserExecution(supervisor.supervisorId, filters);
      setMerchandisers(Array.isArray(result) ? result : []);
    } catch (loadError) {
      setError("Unable to load data. Please check backend connection.");
    } finally {
      setIsLoading(false);
    }
  }

  function handleMerchStoreFormatGroupChange(nextStoreFormatGroup) {
    setMerchStoreFormatGroup(nextStoreFormatGroup);
    loadMerchandisers(nextStoreFormatGroup);
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
            startDate={startDate}
            endDate={endDate}
            onMonthChange={setSelectedMonth}
            onDayChange={setSelectedDay}
            onStartDateChange={setStartDate}
            onEndDateChange={setEndDate}
            onRefresh={loadData}
            onLogout={handleLogout}
            onOpenMerch={() => setActiveScreen("merch")}
            onOpenMap={() => setActiveScreen("map")}
          />
        ) : activeScreen === "merch" ? (
          <MerchandiserExecutionScreen
            supervisorId={supervisor.supervisorId}
            merchandisers={merchandisers}
            overview={overview}
            isLoading={isLoading}
            error={error}
            selectedMonth={selectedMonth}
            selectedDay={selectedDay}
            startDate={startDate}
            endDate={endDate}
            storeFormatGroup={merchStoreFormatGroup}
            onStoreFormatGroupChange={handleMerchStoreFormatGroupChange}
          />
        ) : activeScreen === "issues" ? (
          <IssuesScreen
            issues={issues}
            isLoading={isLoading}
            error={error}
            onOpenStore={(store) => {
              setSelectedStoreDetail(store);
              setStoreDetailBackScreen("issues");
              setActiveScreen("storeDetail");
            }}
          />
        ) : activeScreen === "storeDetail" ? (
          <StoreDetailScreen
            supervisorId={supervisor.supervisorId}
            store={selectedStoreDetail}
            selectedMonth={selectedMonth}
            selectedDay={selectedDay}
            startDate={startDate}
            endDate={endDate}
            onBack={() => setActiveScreen(storeDetailBackScreen)}
          />
        ) : (
          <StoreMapScreen
            supervisorId={supervisor.supervisorId}
            stores={stores}
            isLoading={isLoading}
            error={error}
            selectedStore={selectedStore}
            onSelectStore={setSelectedStore}
            onOpenStoreDetail={(store) => {
              setSelectedStoreDetail(store);
              setStoreDetailBackScreen("map");
              setActiveScreen("storeDetail");
            }}
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
          label="Merch"
          isActive={activeScreen === "merch"}
          onPress={() => setActiveScreen("merch")}
        />
        <TabButton
          label="Stores"
          isActive={activeScreen === "map" || activeScreen === "storeDetail"}
          onPress={() => setActiveScreen("map")}
        />
        <TabButton
          label="Follow-up"
          badge={issues.length > 0 ? String(issues.length) : ""}
          isActive={activeScreen === "issues"}
          onPress={() => setActiveScreen("issues")}
        />
      </View>
      <StatusBar style="dark" />
    </SafeAreaView>
  );
}
