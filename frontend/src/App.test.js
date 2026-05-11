import { render, screen } from "@testing-library/react";
import LoginPage from "./pages/LoginPage";

test("renders the login page before authentication", () => {
  render(<LoginPage onLogin={jest.fn()} />);

  expect(
    screen.getByRole("heading", { name: /Demo Login/i })
  ).toBeInTheDocument();
  expect(screen.getByPlaceholderText(/Enter username/i)).toBeInTheDocument();
  expect(screen.getByPlaceholderText(/Enter password/i)).toBeInTheDocument();
});
