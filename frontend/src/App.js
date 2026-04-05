import React, { useEffect, useState } from "react";

function App() {
  const [employees, setEmployees] = useState([]);

  useEffect(() => {
    fetch("http://localhost:9000/api/employees/")
      .then((res) => res.json())
      .then((data) => {
        console.log(data);
        setEmployees(data);
      })
      .catch((err) => console.error(err));
  }, []);

  return (
    <div>
      <h1>Merch Dashboard</h1>

      <h2>Employees</h2>

      <ul>
        {employees.map((emp) => (
          <li key={emp.employee_id}>
            {emp.employee_code} - {emp.username}
          </li>
        ))}
      </ul>
    </div>
  );
}

export default App;