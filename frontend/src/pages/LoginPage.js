import { useState } from "react";

function LoginPage({ onLogin }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  function handleSubmit(event) {
    event.preventDefault();

    if (!username.trim() || !password.trim()) {
      setError("Please enter username and password.");
      return;
    }

    setError("");
    onLogin(username.trim());
  }

  return (
    <div className="login-page">
      <form className="login-card" onSubmit={handleSubmit}>
        <p className="eyebrow">Merch Performance App</p>
        <h1>Demo Login</h1>
        <p className="login-text">
          This is a demo login to access the store map page.
        </p>

        <label>Username</label>
        <input
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          placeholder="Enter username"
        />

        <label>Password</label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Enter password"
        />

        {error ? <p className="form-error">{error}</p> : null}

        <button type="submit">Login</button>
      </form>
    </div>
  );
}

export default LoginPage;
