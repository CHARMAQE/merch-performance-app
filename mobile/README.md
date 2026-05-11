# Merch Performance Mobile App

This is the native mobile version of the Merch Performance app. It uses Expo and React Native, while the existing Spring Boot backend stays the same.

## Main Files

- `App.js`: coordinates login state, active tab, selected filters, and shared loaded data.
- `src/api/backendApi.js`: contains all calls from the mobile app to the Spring Boot backend.
- `src/screens/LoginScreen.js`: login screen UI.
- `src/screens/DashboardScreen.js`: dashboard screen UI.
- `src/screens/StoreMapScreen.js`: map screen, store search, location, and store detail sheet.
- `src/components/`: small reusable UI parts such as stat cards, filter chips, and tab buttons.
- `src/constants/`: fixed app values such as colors, report year, months, and map region.
- `src/utils/formatters.js`: shared display formatting helpers.
- `src/styles/appStyles.js`: shared React Native styles.

## Mobile Data Flow

```text
App.js
  -> src/api/backendApi.js
  -> Spring Boot backend
  -> App.js stores data in state
  -> screens display the data
```

## How To Run

Start the backend first on port `9000`.

Then run the mobile app:

```bash
cd mobile
npm start
```

Install Expo Go on your phone, then scan the QR code.

## Backend URL

The phone must call your laptop IP address, not `localhost`.

Current URL in `src/api/backendApi.js`:

```text
http://192.168.100.198:9000
```

If your Wi-Fi IP changes, update `LAN_API_BASE` in `src/api/backendApi.js`.

The app uses this same LAN URL for Expo Go on a physical phone.

If the QR code still does not open in Expo Go, try tunnel mode:

```bash
npm run start:tunnel
```
