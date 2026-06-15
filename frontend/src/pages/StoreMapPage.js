import { useEffect, useMemo, useRef, useState } from "react";
import Map from "ol/Map";
import View from "ol/View";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import OSM from "ol/source/OSM";
import VectorSource from "ol/source/Vector";
import Feature from "ol/Feature";
import Point from "ol/geom/Point";
import { fromLonLat, transformExtent } from "ol/proj";
import { Style, Circle as CircleStyle, Fill, Stroke } from "ol/style";
import { defaults as defaultControls } from "ol/control";
import { getProblematicStores } from "../api/storeMapApi";
import ValidationCenterPage from "./ValidationCenterPage";

const MOROCCO_CENTER = fromLonLat([-7.0926, 31.7917]);
const MOROCCO_EXTENT = transformExtent(
  [-13.5, 20.5, -0.8, 36.3],
  "EPSG:4326",
  "EPSG:3857"
);
const MOROCCO_OVERVIEW_ZOOM = 6;
const STORE_FOCUS_ZOOM = 17;
const SITUATION_TABLE_LIMIT = 25;

const CHANNEL_OPTIONS = [
  { value: "ALL", label: "All" },
  { value: "MT", label: "MT - OSA" },
  { value: "GT", label: "GT - GPS" },
];
const defaultMarkerStyle = new Style({
  image: new CircleStyle({
    radius: 10,
    fill: new Fill({ color: "#0079C0" }),
    stroke: new Stroke({ color: "#ffffff", width: 3 }),
  }),
});

const selectedMarkerStyle = new Style({
  image: new CircleStyle({
    radius: 12,
    fill: new Fill({ color: "#004566" }),
    stroke: new Stroke({ color: "#ffffff", width: 3 }),
  }),
});

const issueMarkerStyle = new Style({
  image: new CircleStyle({
    radius: 10,
    fill: new Fill({ color: "#d92d20" }),
    stroke: new Stroke({ color: "#ffffff", width: 3 }),
  }),
});

function forceMapResize(map) {
  if (!map) {
    return;
  }

  const refresh = () => {
    map.updateSize();
    map.renderSync();
  };

  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(refresh);
  });
  window.setTimeout(refresh, 150);
  window.setTimeout(refresh, 500);
}

function formatNumber(value) {
  const numberValue = Number(value);

  if (!Number.isFinite(numberValue)) {
    return "0";
  }

  return new Intl.NumberFormat("en-US").format(numberValue);
}

function channelLabel(channel) {
  if (channel === "MT") {
    return "MT";
  }
  if (channel === "GT") {
    return "GT";
  }
  return "All channels";
}

function searchPlaceholder(channel) {
  if (channel === "MT") {
    return "Search MT OSA problem store...";
  }
  if (channel === "GT") {
    return "Search GT GPS problem store...";
  }
  return "Search store, merchandiser, city, format...";
}

function emptyStateForChannel(channel) {
  if (channel === "GT") {
    return {
      title: "No GT GPS inconsistencies detected",
      message:
        "The latest validation run did not detect GPS consistency issues for GT stores. GT stores are monitored using GPS consistency, while MT stores are monitored using OSA anomaly detection.",
    };
  }

  if (channel === "MT") {
    return {
      title: "No MT OSA anomalies detected",
      message:
        "The latest validation run did not detect OSA anomaly issues for MT stores. MT stores are monitored using OSA anomaly detection.",
    };
  }

  return {
    title: "No situations to review",
    message: "No grouped situations match the selected scope.",
  };
}

function buildSearchText(item) {
  return [
    item?.storeCode,
    item?.storeName,
    item?.merchandiserName,
    item?.employeeCode,
    item?.city,
    item?.storeCity,
    item?.storeFormat,
    item?.channel,
  ]
    .filter((value) => value !== undefined && value !== null)
    .join(" ")
    .toLowerCase();
}

function overviewStatusLabel(row) {
  const status = row?.reviewStatus || row?.review_status;

  if (!status) {
    return "Pending review";
  }

  return String(status)
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function countDistinctStores(rows) {
  return new Set(
    rows
      .map((row) => String(row?.storeCode || "").trim())
      .filter(Boolean)
  ).size;
}

function sumIssueCount(rows) {
  return rows.reduce(
    (total, row) => total + Number(row?.totalIssueCount ?? row?.issueCount ?? 0),
    0
  );
}

function StoreMapPage({ username, onLogout }) {
  const [selectedStore, setSelectedStore] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const [loadingStoreMap, setLoadingStoreMap] = useState(false);
  const [error, setError] = useState("");
  const [problematicStores, setProblematicStores] = useState([]);
  const [activeWorkspace, setActiveWorkspace] = useState("storeMap");
  const [channelFilter, setChannelFilter] = useState("ALL");
  const [dateDraft, setDateDraft] = useState({ startDate: "", endDate: "" });
  const [dateRange, setDateRange] = useState({ startDate: "", endDate: "" });

  const mapWrapperRef = useRef(null);
  const mapElementRef = useRef(null);
  const mapRef = useRef(null);
  const vectorSourceRef = useRef(null);
  const selectedStoreCodeRef = useRef(null);
  const storeIssuesIndexRef = useRef({});

  const problematicRows = useMemo(
    () => (Array.isArray(problematicStores) ? problematicStores : []),
    [problematicStores]
  );
  const reviewableProblematicRows = useMemo(
    () => problematicRows,
    [problematicRows]
  );
  const channelProblematicRows = useMemo(() => {
    if (channelFilter === "ALL") {
      return reviewableProblematicRows;
    }

    return reviewableProblematicRows.filter(
      (row) => String(row?.channel || "").toUpperCase() === channelFilter
    );
  }, [channelFilter, reviewableProblematicRows]);
  const normalizedSearchTerm = searchTerm.trim().toLowerCase();
  const displayedProblematicRows = useMemo(() => {
    if (!normalizedSearchTerm) {
      return channelProblematicRows;
    }

    return channelProblematicRows.filter((row) => {
      return buildSearchText(row).includes(normalizedSearchTerm);
    });
  }, [channelProblematicRows, normalizedSearchTerm]);

  const filteredProblematicStores = useMemo(() => {
    const storesByCode = {};

    displayedProblematicRows.forEach((row) => {
      if (!row || typeof row !== "object") {
        return;
      }

      const storeCode = String(row.storeCode || "").trim();
      const latitude = Number(row.latitude);
      const longitude = Number(row.longitude);

      if (
        !storeCode ||
        row.latitude === null ||
        row.longitude === null ||
        Number.isNaN(latitude) ||
        Number.isNaN(longitude)
      ) {
        return;
      }

      const existingStore = storesByCode[storeCode];
      const nextIssueCount = Number(row.totalIssueCount || 0);
      if (
        existingStore &&
        Number(existingStore.totalIssueCount || 0) >= nextIssueCount
      ) {
        return;
      }

      storesByCode[storeCode] = {
        storeCode,
        storeName: row.storeName || storeCode,
        storeFormat: row.storeFormat,
        merchandiserName: row.merchandiserName,
        city: row.city || row.storeCity,
        channel: row.channel,
        latitude,
        longitude,
        totalIssueCount: nextIssueCount,
      };
    });

    return Object.values(storesByCode).sort(
      (a, b) => Number(b.totalIssueCount || 0) - Number(a.totalIssueCount || 0)
    );
  }, [displayedProblematicRows]);

  const matchingStores = searchTerm.trim()
    ? filteredProblematicStores
        .filter((store) => {
          return buildSearchText(store).includes(searchTerm.trim().toLowerCase());
        })
        .slice(0, 8)
    : [];

  function centerOnStore(store) {
    const map = mapRef.current;
    if (!map || !store) {
      return;
    }

    const coordinate = fromLonLat([
      Number(store.longitude),
      Number(store.latitude),
    ]);

    const view = map.getView();
    const targetZoom = Math.max(view.getZoom() ?? 6, STORE_FOCUS_ZOOM);

    view.animate(
      {
        center: coordinate,
        zoom: targetZoom,
        duration: 250,
      },
      () => {
        view.setCenter(coordinate);
        view.setZoom(targetZoom);
        forceMapResize(map);
      }
    );
    forceMapResize(map);
  }

  function updateChannelFilter(nextChannel) {
    setChannelFilter(nextChannel);
    setSelectedStore(null);
    selectedStoreCodeRef.current = null;
    setSearchTerm("");
    setIsSearchOpen(false);
  }

  function focusStore(store) {
    if (!store) {
      return;
    }

    setSelectedStore(store);
    selectedStoreCodeRef.current = store.storeCode;
    setSearchTerm(store.storeName);
    setIsSearchOpen(false);

    centerOnStore(store);
    vectorSourceRef.current?.changed();
  }

  function handleSearchSubmit(event) {
    event.preventDefault();

    if (matchingStores.length > 0) {
      focusStore(matchingStores[0]);
    }
  }

  function applyDateRange(event) {
    event.preventDefault();

    if (
      dateDraft.startDate &&
      dateDraft.endDate &&
      dateDraft.startDate > dateDraft.endDate
    ) {
      setError("Start date cannot be after end date.");
      return;
    }

    setDateRange(dateDraft);
    setSelectedStore(null);
    selectedStoreCodeRef.current = null;
    setSearchTerm("");
    setIsSearchOpen(false);
  }

  function resetDateRange() {
    const emptyRange = { startDate: "", endDate: "" };
    setDateDraft(emptyRange);
    setDateRange(emptyRange);
    setSelectedStore(null);
    selectedStoreCodeRef.current = null;
    setSearchTerm("");
    setIsSearchOpen(false);
  }

  useEffect(() => {
    let cancelled = false;

    async function loadStoreMapData() {
      try {
        setLoadingStoreMap(true);
        setError("");

        const data = await getProblematicStores({
          limit: 5000,
          startDate: dateRange.startDate,
          endDate: dateRange.endDate,
        });

        if (!cancelled) {
          setProblematicStores(Array.isArray(data?.rows) ? data.rows : []);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError.message || "Unable to load problematic stores.");
          setProblematicStores([]);
        }
      } finally {
        if (!cancelled) {
          setLoadingStoreMap(false);
        }
      }
    }

    loadStoreMapData();

    return () => {
      cancelled = true;
    };
  }, [dateRange.startDate, dateRange.endDate]);

  useEffect(() => {
    const handleResize = () => {
      forceMapResize(mapRef.current);
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, []);

  useEffect(() => {
    if (!mapElementRef.current || mapRef.current) {
      return;
    }

    let resizeObserver;

    const vectorSource = new VectorSource();
    vectorSourceRef.current = vectorSource;

    const vectorLayer = new VectorLayer({
      source: vectorSource,
      style: (feature) => {
        const storeData = feature.get("storeData");
        const storeCode = storeData?.storeCode;
        const hasIssues = Boolean(
          storeCode && storeIssuesIndexRef.current[storeCode]
        );
        return storeData?.storeCode === selectedStoreCodeRef.current
          ? selectedMarkerStyle
          : hasIssues
            ? issueMarkerStyle
            : defaultMarkerStyle;
      },
    });

    const map = new Map({
      target: mapElementRef.current,
      layers: [
        new TileLayer({
          preload: 3,
          useInterimTilesOnError: false,
          cacheSize: 2048,
          source: new OSM({
            interpolate: false,
            transition: 0,
            zDirection: -1,
          }),
        }),
        vectorLayer,
      ],
      view: new View({
        center: MOROCCO_CENTER,
        zoom: MOROCCO_OVERVIEW_ZOOM,
        minZoom: 5,
        maxZoom: 18,
        extent: MOROCCO_EXTENT,
        constrainOnlyCenter: true,
        constrainResolution: true,
        smoothResolutionConstraint: false,
      }),
      controls: defaultControls(),
    });

    map.on("singleclick", (event) => {
      const feature = map.forEachFeatureAtPixel(
        event.pixel,
        (clickedFeature) => clickedFeature,
        { hitTolerance: 18 }
      );

      if (!feature) {
        setSelectedStore(null);
        selectedStoreCodeRef.current = null;
        setSearchTerm("");
        setIsSearchOpen(false);
        vectorSource.changed();
        return;
      }

      const storeData = feature.get("storeData");
      setSelectedStore(storeData);
      selectedStoreCodeRef.current = storeData.storeCode;
      setSearchTerm(storeData.storeName);
      setIsSearchOpen(false);
      vectorSource.changed();
    });

    resizeObserver = new ResizeObserver(() => {
      forceMapResize(map);
    });
    if (mapWrapperRef.current) {
      resizeObserver.observe(mapWrapperRef.current);
    }

    mapRef.current = map;
    forceMapResize(map);

    return () => {
      resizeObserver?.disconnect();
      map.setTarget(undefined);
      mapRef.current = null;
      vectorSourceRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const vectorSource = vectorSourceRef.current;

    if (!map || !vectorSource) {
      return;
    }

    const markerStores = selectedStore
      ? [selectedStore]
      : filteredProblematicStores;

    const features = markerStores.map((store) => {
      const feature = new Feature({
        geometry: new Point(
          fromLonLat([Number(store.longitude), Number(store.latitude)])
        ),
      });

      feature.set("storeData", store);
      return feature;
    });

    vectorSource.clear();
    vectorSource.addFeatures(features);
    vectorSource.changed();

    if (selectedStore && markerStores.length === 1) {
      centerOnStore(markerStores[0]);
    } else {
      map.getView().setCenter(MOROCCO_CENTER);
      map.getView().setZoom(MOROCCO_OVERVIEW_ZOOM);
      forceMapResize(map);
    }
  }, [filteredProblematicStores, selectedStore]);

  useEffect(() => {
    selectedStoreCodeRef.current = selectedStore?.storeCode ?? null;
    vectorSourceRef.current?.changed();
  }, [selectedStore]);

  useEffect(() => {
    if (activeWorkspace === "storeMap") {
      forceMapResize(mapRef.current);
    }
  }, [activeWorkspace]);

  useEffect(() => {
    const nextIndex = displayedProblematicRows.reduce((accumulator, row) => {
      const storeCode = row.storeCode;
      if (!storeCode) {
        return accumulator;
      }

      accumulator[storeCode] =
        (accumulator[storeCode] || 0) + Number(row.totalIssueCount || 0);
      return accumulator;
    }, {});

    storeIssuesIndexRef.current = nextIndex;
    vectorSourceRef.current?.changed();
  }, [displayedProblematicRows]);

  const detectedIssueRows = (selectedStore
    ? displayedProblematicRows.filter(
        (row) => row.storeCode === selectedStore.storeCode
      )
    : [...displayedProblematicRows]
  ).sort((a, b) => {
    if (Number(b.totalIssueCount || 0) !== Number(a.totalIssueCount || 0)) {
      return Number(b.totalIssueCount || 0) - Number(a.totalIssueCount || 0);
    }
    return String(b.visitDate || "").localeCompare(String(a.visitDate || ""));
  });
  const visibleDetectedIssueRows = detectedIssueRows.slice(0, SITUATION_TABLE_LIMIT);
  const detectedIssueRowCount = sumIssueCount(detectedIssueRows);
  const mtRows = reviewableProblematicRows.filter(
    (row) => String(row?.channel || "").toUpperCase() === "MT"
  );
  const gtRows = reviewableProblematicRows.filter(
    (row) => String(row?.channel || "").toUpperCase() === "GT"
  );
  const mtStoreCount = countDistinctStores(mtRows);
  const gtStoreCount = countDistinctStores(gtRows);
  const allStoreCount = countDistinctStores(reviewableProblematicRows);
  const activeSituationCount = sumIssueCount(detectedIssueRows);
  const activeStoreCount =
    channelFilter === "MT"
      ? mtStoreCount
      : channelFilter === "GT"
        ? gtStoreCount
        : allStoreCount;
  const activeEmptyState = emptyStateForChannel(channelFilter);
  const summaryProblemStoreCount = selectedStore
    ? 1
    : activeStoreCount;
  const kpiCards = [
    {
      label: "Situations to Review",
      value: activeSituationCount,
      helper: selectedStore ? "Selected store" : channelLabel(channelFilter),
      tone: "total",
    },
    {
      label: "Problematic Stores",
      value: summaryProblemStoreCount,
      helper: selectedStore ? "Focused store" : "Distinct stores",
      tone: "stores",
    },
  ];
  const tableEmptyState = normalizedSearchTerm
    ? {
        title: "No matching problem stores",
        message:
          "No grouped situations match the current search for this channel.",
      }
    : activeEmptyState;
  return (
    <div className="dashboard-page">
      <header className="workspace-header">
        <div className="workspace-brand">
          <p className="eyebrow">Smollan / Unilever</p>
          <h1>Back-office Monitoring</h1>
        </div>
        <nav className="workspace-tabs" aria-label="Back-office workspace">
          <button
            type="button"
            className={activeWorkspace === "storeMap" ? "is-active" : ""}
            onClick={() => setActiveWorkspace("storeMap")}
          >
            Problem Stores Overview
          </button>
          <button
            type="button"
            className={activeWorkspace === "validationCenter" ? "is-active" : ""}
            onClick={() => setActiveWorkspace("validationCenter")}
          >
            Validation Center
          </button>
        </nav>
        <div className="header-channel-filter">
          <div className="channel-tabs header-channel-tabs" aria-label="Channel filter">
            {CHANNEL_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                className={channelFilter === option.value ? "is-active" : ""}
                onClick={() => updateChannelFilter(option.value)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>
        <button className="logout-button" onClick={onLogout}>
          Logout
        </button>
      </header>

      <main
        className={`problem-overview-page workspace-panel ${
          activeWorkspace === "storeMap" ? "is-active" : "is-hidden"
        }`}
      >
        <section className="overview-kpi-grid" aria-label="Problem store indicators">
          {kpiCards.map((card) => (
            <div
              className={`overview-kpi-card is-${card.tone} ${
                card.muted ? "is-muted" : ""
              }`}
              key={card.label}
            >
              <span>{card.label}</span>
              <strong>
                {card.value === null ? "-" : formatNumber(card.value)}
              </strong>
              <small>{card.helper}</small>
            </div>
          ))}
          <form className="overview-date-card" onSubmit={applyDateRange}>
            <div>
              <span>Date Range</span>
              <small>Filter latest run by visit date</small>
            </div>
            <div className="overview-date-inputs">
              <input
                aria-label="Start date"
                type="date"
                value={dateDraft.startDate}
                onChange={(event) =>
                  setDateDraft((current) => ({
                    ...current,
                    startDate: event.target.value,
                  }))
                }
              />
              <input
                aria-label="End date"
                type="date"
                value={dateDraft.endDate}
                onChange={(event) =>
                  setDateDraft((current) => ({
                    ...current,
                    endDate: event.target.value,
                  }))
                }
              />
            </div>
            <div className="overview-date-actions">
              <button type="submit" className="map-search-button">
                Apply
              </button>
              <button
                type="button"
                className="map-secondary-button"
                onClick={resetDateRange}
              >
                Reset
              </button>
            </div>
          </form>
        </section>

        <section className="overview-content-grid">
          <section className="problem-table-panel" aria-label="Problematic stores">
            <div className="problem-table-header">
              <div>
                <p className="eyebrow">Decision support</p>
                <h2>Situation Review Queue</h2>
                <span className="issue-table-note">
                  Showing top {Math.min(SITUATION_TABLE_LIMIT, detectedIssueRows.length)} of{" "}
                  {formatNumber(detectedIssueRowCount)} situations.
                </span>
              </div>
              <span className="status-pill">
                {loadingStoreMap ? "Loading" : error ? "Error" : "Latest run"}
              </span>
            </div>

            {error ? <div className="validation-error">{error}</div> : null}

            <div className="problem-table-toolbar">
              <div className="problem-search-shell">
                <form className="map-search-form" onSubmit={handleSearchSubmit}>
                  <input
                    type="text"
                    value={searchTerm}
                    onChange={(event) => {
                      const nextValue = event.target.value;
                      setSearchTerm(nextValue);
                      setIsSearchOpen(true);
                      setSelectedStore(null);
                      selectedStoreCodeRef.current = null;
                    }}
                    onFocus={() => setIsSearchOpen(true)}
                    onBlur={() => {
                      window.setTimeout(() => {
                        setIsSearchOpen(false);
                      }, 150);
                    }}
                    className="map-search-input"
                    placeholder={searchPlaceholder(channelFilter)}
                  />
                  <button type="submit" className="map-search-button">
                    Search
                  </button>
                </form>

                {isSearchOpen && matchingStores.length > 0 ? (
                  <div className="search-results">
                    {matchingStores.map((store) => (
                      <button
                        key={store.storeCode}
                        type="button"
                        className="search-result-button"
                        onMouseDown={(event) => {
                          event.preventDefault();
                          focusStore(store);
                        }}
                      >
                        <strong>{store.storeName}</strong>
                        <span>{store.storeCode}</span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>
            </div>

            {loadingStoreMap ? (
              <div className="loading-box">Loading situations...</div>
            ) : null}

            {detectedIssueRows.length > 0 ? (
              <div className="issues-table overview-situation-table">
                <div className="issues-head">
                  <span>Store</span>
                  <span>Merchandiser</span>
                  <span>Suspicious responses</span>
                  <span>Status</span>
                  <span>Action</span>
                </div>
                {visibleDetectedIssueRows.map((row) => (
                  <div
                    className="issues-row"
                    key={`${row.employeeCode}-${row.storeCode}-${row.visitDate || "no-date"}`}
                  >
                    <span>
                      <strong>{row.storeName}</strong>
                      <small>{row.storeCode}</small>
                    </span>
                    <span>
                      <strong>{row.merchandiserName || row.employeeCode}</strong>
                      <small>{row.employeeCode || "Employee code unavailable"}</small>
                    </span>
                    <span>
                      <strong>{formatNumber(row.totalIssueCount)}</strong>
                      <small>
                        OSA {formatNumber(row.osaIssueCount)} / GPS {formatNumber(row.gpsIssueCount)}
                      </small>
                    </span>
                    <span>
                      {overviewStatusLabel(row)}
                    </span>
                    <span>
                      <button
                        type="button"
                        className="review-action-button"
                        onClick={() => setActiveWorkspace("validationCenter")}
                      >
                        Review
                      </button>
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="store-map-empty-state">
                <h3>{tableEmptyState.title}</h3>
                <p>{tableEmptyState.message}</p>
              </div>
            )}
          </section>

          <aside className="support-map-panel" aria-label="Geographic view">
            <div className="map-panel-header">
              <div>
                <p className="eyebrow">Geographic view</p>
                <h2>Geographic View</h2>
                {selectedStore ? (
                  <span className="issue-table-note">{selectedStore.storeName}</span>
                ) : null}
              </div>
            </div>

            {error ? <div className="error-box">{error}</div> : null}

            <div ref={mapWrapperRef} className="map-wrapper support-map-wrapper">
              <div ref={mapElementRef} className="map-canvas" />
            </div>

            <div className="map-context-panel">
              <span>{formatNumber(filteredProblematicStores.length)} mapped stores</span>
            </div>
          </aside>
        </section>
      </main>

      <section
        className={`workspace-panel ${
          activeWorkspace === "validationCenter" ? "is-active" : "is-hidden"
        }`}
      >
        <ValidationCenterPage username={username} selectedChannel={channelFilter} />
      </section>
    </div>
  );
}

export default StoreMapPage;
