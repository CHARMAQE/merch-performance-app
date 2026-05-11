import "./styles/app.css";
import { useState } from "react";
import LoginPage from "./pages/LoginPage";
import StoreMapPage from "./pages/StoreMapPage";

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(
    () => localStorage.getItem("demo-auth") === "true"
  );
  const [username, setUsername] = useState(
    () => localStorage.getItem("demo-user") || ""
  );

  function handleLogin(user) {
    localStorage.setItem("demo-auth", "true");
    localStorage.setItem("demo-user", user);
    setUsername(user);
    setIsLoggedIn(true);
  }

  function handleLogout() {
    localStorage.removeItem("demo-auth");
    localStorage.removeItem("demo-user");
    setUsername("");
    setIsLoggedIn(false);
  }

  if (!isLoggedIn) {
    return <LoginPage onLogin={handleLogin} />;
  }

  return <StoreMapPage username={username} onLogout={handleLogout} />;
}

export default App;
