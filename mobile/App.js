import { StatusBar } from "expo-status-bar";
import { useEffect, useState } from "react";
import { SafeAreaView, View } from "react-native";
import {
  getSupervisorDashboardOverview,
  getSupervisorMerchandiserExecution,
  getSupervisorStores,
} from "./src/api/backendApi";
import { TabButton } from "./src/components/TabButton";
import { DEFAULT_REPORT_MONTH, REPORT_YEAR } from "./src/constants/appConstants";
import { DashboardScreen } from "./src/screens/DashboardScreen";
import { LoginScreen } from "./src/screens/LoginScreen";
import { MerchandiserExecutionScreen } from "./src/screens/MerchandiserExecutionScreen";
import { StoreDetailScreen } from "./src/screens/StoreDetailScreen";
import { StoreMapScreen } from "./src/screens/StoreMapScreen";
import { SupervisorProfileScreen } from "./src/screens/SupervisorProfileScreen";
import { TasksOverviewScreen } from "./src/screens/TasksOverviewScreen";
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
  const [selectedMerchandiser, setSelectedMerchandiser] = useState(null);
  const [selectedMerchandiserStores, setSelectedMerchandiserStores] = useState([]);
  const [selectedTaskVisit, setSelectedTaskVisit] = useState(null);
  const [tasksOverviewBackScreen, setTasksOverviewBackScreen] = useState("merch");
  const [stores, setStores] = useState([]);
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
    setSelectedMerchandiser(null);
    setSelectedMerchandiserStores([]);
    setSelectedTaskVisit(null);
    setTasksOverviewBackScreen("merch");
    setStores([]);
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

      const [overviewResult, merchandisersResult, storesResult] = await Promise.allSettled([
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

  function handleOpenTasksOverview(storeVisit, merchandiser) {
    setSelectedTaskVisit({ storeVisit, merchandiser });
    setTasksOverviewBackScreen("merch");
    setActiveScreen("tasksOverview");
  }

  function handleOpenStoreExecution(store) {
    const visitId = store?.latestVisitId || store?.visitId;

    if (!visitId) {
      setSelectedStoreDetail(store);
      setStoreDetailBackScreen("map");
      setActiveScreen("storeDetail");
      return;
    }

    setSelectedTaskVisit({
      storeVisit: {
        ...store,
        visitId,
        visitDate: store.latestVisitDate || store.visitDate,
      },
      merchandiser: {
        username: store.merchandiserName || store.username,
        name: store.merchandiserName || store.username,
      },
    });
    setTasksOverviewBackScreen("map");
    setActiveScreen("tasksOverview");
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
            onOpenProfile={() => setActiveScreen("profile")}
            onOpenMerch={() => setActiveScreen("merch")}
            onOpenMap={() => setActiveScreen("map")}
          />
        ) : activeScreen === "profile" ? (
          <SupervisorProfileScreen
            supervisor={supervisor}
            overview={overview}
            stores={stores}
            merchandisers={merchandisers}
            onBack={() => setActiveScreen("home")}
          />
        ) : activeScreen === "merch" ? (
          <MerchandiserExecutionScreen
            supervisorId={supervisor.supervisorId}
            merchandisers={merchandisers}
            selectedMerchandiser={selectedMerchandiser}
            selectedMerchandiserStores={selectedMerchandiserStores}
            onSelectedMerchandiserChange={setSelectedMerchandiser}
            onSelectedMerchandiserStoresChange={setSelectedMerchandiserStores}
            onOpenTasksOverview={handleOpenTasksOverview}
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
        ) : activeScreen === "tasksOverview" ? (
          <TasksOverviewScreen
            supervisorId={supervisor.supervisorId}
            visitId={selectedTaskVisit?.storeVisit?.visitId}
            storeVisit={selectedTaskVisit?.storeVisit}
            merchandiser={selectedTaskVisit?.merchandiser}
            onBack={() => setActiveScreen(tasksOverviewBackScreen)}
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
            onOpenStoreExecution={handleOpenStoreExecution}
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
          isActive={activeScreen === "home" || activeScreen === "profile"}
          onPress={() => setActiveScreen("home")}
        />
        <TabButton
          label="Merch"
          isActive={activeScreen === "merch" || activeScreen === "tasksOverview"}
          onPress={() => setActiveScreen("merch")}
        />
        <TabButton
          label="Stores"
          isActive={activeScreen === "map" || activeScreen === "storeDetail"}
          onPress={() => setActiveScreen("map")}
        />
      </View>
      <StatusBar style="dark" />
    </SafeAreaView>
  );
}
