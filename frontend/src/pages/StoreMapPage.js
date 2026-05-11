import { useEffect, useRef, useState } from "react";
import Map from "ol/Map";
import View from "ol/View";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import OSM from "ol/source/OSM";
import VectorSource from "ol/source/Vector";
import Feature from "ol/Feature";
import Point from "ol/geom/Point";
import { fromLonLat } from "ol/proj";
import { Style, Circle as CircleStyle, Fill, Stroke } from "ol/style";
import { defaults as defaultControls } from "ol/control";
import {
  fetchDashboardOverviewForStore,
  fetchLatestValidationIssues,
} from "../api/dashboardApi";
import { fetchStoreDetails, fetchStoreMarkers } from "../api/storeMapApi";

const MOROCCO_CENTER = fromLonLat([-7.0926, 31.7917]);
const STORE_FOCUS_ZOOM = 17;

const defaultMarkerStyle = new Style({
  image: new CircleStyle({
    radius: 10,
    fill: new Fill({ color: "#07045f" }),
    stroke: new Stroke({ color: "#ffffff", width: 3 }),
  }),
});

const selectedMarkerStyle = new Style({
  image: new CircleStyle({
    radius: 12,
    fill: new Fill({ color: "#f97316" }),
    stroke: new Stroke({ color: "#ffffff", width: 3 }),
  }),
});

const issueMarkerStyle = new Style({
  image: new CircleStyle({
    radius: 10,
    fill: new Fill({ color: "#b42318" }),
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
  if (typeof value !== "number") {
    return "0";
  }

  return new Intl.NumberFormat("en-US").format(value);
}

function formatDateTime(value) {
  if (!value) {
    return "N/A";
  }

  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatPercentage(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "N/A";
  }

  return `${Math.round(Number(value))}%`;
}

function StoreMapPage({ username, onLogout }) {
  const [allStores, setAllStores] = useState([]);
  const [visibleStores, setVisibleStores] = useState([]);
  const [selectedStore, setSelectedStore] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState("");
  const [validationOverview, setValidationOverview] = useState(null);
  const [validationIssues, setValidationIssues] = useState([]);
  const [isValidationLoading, setIsValidationLoading] = useState(true);
  const [validationError, setValidationError] = useState("");
  const [storeDetails, setStoreDetails] = useState(null);
  const [isStoreDetailsLoading, setIsStoreDetailsLoading] = useState(false);
  const [storeDetailsError, setStoreDetailsError] = useState("");

  const mapWrapperRef = useRef(null);
  const mapElementRef = useRef(null);
  const mapRef = useRef(null);
  const vectorSourceRef = useRef(null);
  const allStoresRef = useRef([]);
  const selectedStoreCodeRef = useRef(null);
  const storeIssuesIndexRef = useRef({});

  const matchingStores = searchTerm.trim()
    ? allStores
        .filter((store) => {
          const haystack = `${store.storeName} ${store.storeCode}`.toLowerCase();
          return haystack.includes(searchTerm.trim().toLowerCase());
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

  function resetStoreFilter() {
    setVisibleStores(allStores);
    setSelectedStore(null);
    selectedStoreCodeRef.current = null;
    setSearchTerm("");
    setIsSearchOpen(false);
  }

  function focusStore(store, isolateStore) {
    if (!store) {
      return;
    }

    setSelectedStore(store);
    selectedStoreCodeRef.current = store.storeCode;
    setSearchTerm(store.storeName);
    setIsSearchOpen(false);

    if (isolateStore) {
      setVisibleStores([store]);
      return;
    }

    centerOnStore(store);
    vectorSourceRef.current?.changed();
  }

  function handleSearchSubmit(event) {
    event.preventDefault();

    if (matchingStores.length > 0) {
      focusStore(matchingStores[0], true);
    }
  }

  useEffect(() => {
    allStoresRef.current = allStores;
  }, [allStores]);

  useEffect(() => {
    let cancelled = false;

    async function loadStores() {
      try {
        setIsLoading(true);
        setError("");

        const data = await fetchStoreMarkers();
        const validStores = Array.isArray(data)
          ? data.filter(
              (store) =>
                store.latitude !== null &&
                store.longitude !== null &&
                !Number.isNaN(Number(store.latitude)) &&
                !Number.isNaN(Number(store.longitude))
            )
          : [];

        if (!cancelled) {
          setAllStores(validStores);
          setVisibleStores(validStores);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError.message || "Unexpected map loading error.");
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    loadStores();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const activeStoreCode = selectedStore?.storeCode;

    async function loadValidationResults() {
      try {
        setIsValidationLoading(true);
        setValidationError("");

        const [overview, issues] = await Promise.all([
          fetchDashboardOverviewForStore(activeStoreCode),
          fetchLatestValidationIssues(500, activeStoreCode),
        ]);

        if (!cancelled) {
          setValidationOverview(overview);
          setValidationIssues(Array.isArray(issues) ? issues : []);
        }
      } catch (loadError) {
        if (!cancelled) {
          setValidationError(
            loadError.message || "Unexpected validation loading error."
          );
        }
      } finally {
        if (!cancelled) {
          setIsValidationLoading(false);
        }
      }
    }

    loadValidationResults();

    return () => {
      cancelled = true;
    };
  }, [selectedStore?.storeCode]);

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
    let cancelled = false;

    async function loadStoreDetails() {
      if (!selectedStore?.storeCode) {
        setStoreDetails(null);
        setStoreDetailsError("");
        setIsStoreDetailsLoading(false);
        return;
      }

      try {
        setIsStoreDetailsLoading(true);
        setStoreDetailsError("");
        const details = await fetchStoreDetails(selectedStore.storeCode);

        if (!cancelled) {
          setStoreDetails(details);
        }
      } catch (loadError) {
        if (!cancelled) {
          setStoreDetails(null);
          setStoreDetailsError(
            loadError.message || "Unable to load selected store results."
          );
        }
      } finally {
        if (!cancelled) {
          setIsStoreDetailsLoading(false);
        }
      }
    }

    loadStoreDetails();

    return () => {
      cancelled = true;
    };
  }, [selectedStore]);

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
        zoom: 6,
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
        setVisibleStores(allStoresRef.current);
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
      setVisibleStores([storeData]);
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

    if (!map || !vectorSource || allStores.length === 0) {
      return;
    }

    const markerStores = visibleStores.length > 0 ? visibleStores : allStores;

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

    if (markerStores.length === 1) {
      centerOnStore(markerStores[0]);
    } else if (features.length > 0) {
      const extent = vectorSource.getExtent();
      map.getView().fit(extent, {
        padding: [50, 50, 50, 50],
        maxZoom: 12,
        nearest: true,
        duration: 0,
      });
      forceMapResize(map);
    } else {
      map.getView().setCenter(MOROCCO_CENTER);
      map.getView().setZoom(6);
      forceMapResize(map);
    }
  }, [allStores, visibleStores]);

  useEffect(() => {
    selectedStoreCodeRef.current = selectedStore?.storeCode ?? null;
    vectorSourceRef.current?.changed();
  }, [selectedStore]);

  useEffect(() => {
    const nextIndex = validationIssues.reduce((accumulator, issue) => {
      const storeCode = issue.storeCode;
      if (!storeCode) {
        return accumulator;
      }

      accumulator[storeCode] = (accumulator[storeCode] || 0) + 1;
      return accumulator;
    }, {});

    storeIssuesIndexRef.current = nextIndex;
    vectorSourceRef.current?.changed();
  }, [validationIssues]);

  const canShowAllStores = visibleStores.length === 1 && allStores.length > 1;
  const latestRun = validationOverview?.latestValidationRun;
  const ruleResults = validationOverview?.issueCountsByRule ?? [];
  const severityResults = validationOverview?.issueCountsBySeverity ?? [];
  const activeIssueCount = ruleResults.reduce(
    (total, rule) => total + Number(rule.issueCount || 0),
    0
  );
  const issuesByRule = validationIssues.reduce((accumulator, issue) => {
    const rule = issue.ruleCode || "Unknown";
    if (!accumulator[rule]) {
      accumulator[rule] = {
        total: 0,
        LOW: 0,
        MEDIUM: 0,
        HIGH: 0,
        CRITICAL: 0,
      };
    }

    const severity = (issue.severity || "").toUpperCase();
    accumulator[rule].total += 1;
    if (severity && accumulator[rule][severity] !== undefined) {
      accumulator[rule][severity] += 1;
    }

    return accumulator;
  }, {});
  const selectedStoreIssues = selectedStore
    ? validationIssues.filter(
        (issue) => issue.storeCode === selectedStore.storeCode
      )
    : validationIssues;
  const groupedIssuesByMerch = selectedStoreIssues.reduce(
    (accumulator, issue) => {
      const merch = issue.employeeCode || "Unknown";
      const rule = issue.ruleCode || "Unknown";
      const ruleLabel =
        rule === "GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH"
          ? "GPS"
          : rule === "OSA_UNUSUAL_NON_BY_BANNER"
            ? "OSA"
            : rule;

      if (!accumulator[merch]) {
        accumulator[merch] = { total: 0, types: new Set() };
      }

      accumulator[merch].total += 1;
      accumulator[merch].types.add(ruleLabel);
      return accumulator;
    },
    {}
  );
  const merchIssueRows = Object.entries(groupedIssuesByMerch).map(
    ([merch, data]) => ({
      merch,
      total: data.total,
      types: Array.from(data.types).join(", "),
    })
  );
  return (
    <div className="dashboard-page">
      <main className="dashboard-layout">
        <section className="filter-bar" aria-label="Store filter">
          <div className="filter-title">
            <p className="eyebrow">Filter Data</p>
            <h1>{selectedStore ? selectedStore.storeName : "Validation dashboard"}</h1>
            <span>
              {selectedStore
                ? `${selectedStore.storeCode} selected`
                : `${formatNumber(allStores.length)} stores`}
            </span>
          </div>

          <div className="filter-controls">
            <form className="map-search-form" onSubmit={handleSearchSubmit}>
              <input
                type="text"
                value={searchTerm}
                onChange={(event) => {
                  const nextValue = event.target.value;
                  setSearchTerm(nextValue);
                  setIsSearchOpen(true);

                  if (!nextValue.trim()) {
                    setVisibleStores(allStores);
                    setSelectedStore(null);
                    selectedStoreCodeRef.current = null;
                  }
                }}
                onFocus={() => setIsSearchOpen(true)}
                onBlur={() => {
                  window.setTimeout(() => {
                    setIsSearchOpen(false);
                  }, 150);
                }}
                className="map-search-input"
                placeholder="Search store name or code"
              />
              <button type="submit" className="map-search-button">
                Apply
              </button>
              {canShowAllStores ? (
                <button
                  type="button"
                  className="map-secondary-button"
                  onClick={resetStoreFilter}
                >
                  All stores
                </button>
              ) : null}
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
                      focusStore(store, true);
                    }}
                  >
                    <strong>{store.storeName}</strong>
                    <span>{store.storeCode}</span>
                  </button>
                ))}
              </div>
            ) : null}
          </div>

          <button className="logout-button" onClick={onLogout}>
            Logout
          </button>
        </section>

        <section className="dashboard-panel" aria-label="Validation results">
          <div className="panel-card">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Validation results</p>
                <h2>Latest run</h2>
              </div>
              <span className="status-pill">
                {isValidationLoading ? "Loading" : latestRun?.status || "No run"}
              </span>
            </div>

            {validationError ? (
              <div className="validation-error">{validationError}</div>
            ) : null}

            <div className="summary-grid">
              <div className="summary-card">
                <span>Run</span>
                <strong>
                  {latestRun?.runId ? `#${latestRun.runId}` : "N/A"}
                </strong>
                <small>{formatDateTime(latestRun?.finishedAt)}</small>
              </div>
              <div className="summary-card">
                <span>Rules executed</span>
                <strong>{formatNumber(latestRun?.rulesExecuted)}</strong>
                <small>methods checked</small>
              </div>
              <div className="summary-card">
                <span>Issues found</span>
                <strong>
                  {selectedStore
                    ? formatNumber(activeIssueCount)
                    : formatNumber(latestRun?.issuesFound)}
                </strong>
                <small>
                  {selectedStore ? "for this store" : "detected anomalies"}
                </small>
              </div>
            </div>
          </div>

          <div className="panel-card">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Store focus</p>
                <h2>{selectedStore ? "Selected store" : "All stores"}</h2>
              </div>
            </div>

            {selectedStore ? (
              <>
                {storeDetailsError ? (
                  <div className="validation-error">{storeDetailsError}</div>
                ) : null}

                {isStoreDetailsLoading ? (
                  <div className="loading-inline">Loading store details...</div>
                ) : null}

                {!isStoreDetailsLoading && !storeDetailsError ? (
                  <div className="summary-grid">
                    <div className="summary-card">
                      <span>Monthly visits</span>
                      <strong>
                        {formatNumber(storeDetails?.monthlyVisitCount)}
                      </strong>
                    </div>
                    <div className="summary-card">
                      <span>Deviation</span>
                      <strong>
                        {formatPercentage(storeDetails?.deviationPercentage)}
                      </strong>
                    </div>
                    <div className="summary-card">
                      <span>Coverage</span>
                      <strong>
                        {formatPercentage(
                          storeDetails?.coverageRatePercentage
                        )}
                      </strong>
                    </div>
                    <div className="summary-card">
                      <span>OSA</span>
                      <strong>
                        {formatPercentage(storeDetails?.osaPercentage)}
                      </strong>
                    </div>
                  </div>
                ) : null}

                <div className="store-meta-grid">
                  <div>
                    <span>Merch</span>
                    <strong>{storeDetails?.merchandiserName || "N/A"}</strong>
                  </div>
                  <div>
                    <span>Last visit</span>
                    <strong>{formatDateTime(storeDetails?.lastVisitDate)}</strong>
                  </div>
                  <div>
                    <span>City</span>
                    <strong>{selectedStore.storeCity || "N/A"}</strong>
                  </div>
                  <div>
                    <span>Format</span>
                    <strong>{selectedStore.storeFormat || "N/A"}</strong>
                  </div>
                </div>
              </>
            ) : (
              <div className="validation-results-grid">
                <div className="validation-list">
                  <h3>Results by rule</h3>
                  {ruleResults.length > 0 ? (
                    ruleResults.map((rule) => {
                      const ruleStats = issuesByRule[rule.label] || {
                        total: rule.issueCount || 0,
                        LOW: 0,
                        MEDIUM: 0,
                        HIGH: 0,
                        CRITICAL: 0,
                      };

                      return (
                        <div className="rule-row" key={rule.label}>
                          <div>
                            <span className="rule-name" title={rule.label}>
                              {rule.label}
                            </span>
                            <div className="rule-severity">
                              <span>Low {ruleStats.LOW}</span>
                              <span>Med {ruleStats.MEDIUM}</span>
                              <span>High {ruleStats.HIGH}</span>
                              <span>Crit {ruleStats.CRITICAL}</span>
                            </div>
                          </div>
                          <strong>{formatNumber(rule.issueCount)}</strong>
                        </div>
                      );
                    })
                  ) : (
                    <p className="validation-empty">No rule issues found.</p>
                  )}
                </div>

                <div className="validation-list">
                  <h3>Results by severity</h3>
                  {severityResults.length > 0 ? (
                    severityResults.map((severity) => (
                      <div className="validation-row" key={severity.label}>
                        <span>{severity.label}</span>
                        <strong>{formatNumber(severity.issueCount)}</strong>
                      </div>
                    ))
                  ) : (
                    <p className="validation-empty">
                      No severity issues found.
                    </p>
                  )}
                </div>
              </div>
            )}
          </div>

        </section>

        <section className="map-panel" aria-label="Store map">
          <div className="map-panel-header">
            <div>
              <p className="eyebrow">Store map</p>
              <h2>{selectedStore ? selectedStore.storeName : "All stores"}</h2>
              <p className="topbar-text">
                Search or click a marker to focus the store results.
              </p>
            </div>
          </div>

          {error ? <div className="error-box">{error}</div> : null}
          {isLoading ? (
            <div className="loading-box">Loading store map...</div>
          ) : null}

          <div ref={mapWrapperRef} className="map-wrapper">
            <div ref={mapElementRef} className="map-canvas" />
          </div>
        </section>

        <section className="issues-table-section" aria-label="Detected issues">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Detected issues</p>
              <h2>
                {selectedStore
                  ? "Issues for selected store"
                  : "Latest detected issues"}
              </h2>
            </div>
          </div>

          {selectedStoreIssues.length > 0 ? (
            <div className="issues-table">
              <div className="issues-head">
                <span>Merch</span>
                <span>No</span>
                <span>Types</span>
              </div>
              {merchIssueRows.map((row) => (
                <div className="issues-row" key={row.merch}>
                  <span>{row.merch}</span>
                  <span>{row.total}</span>
                  <span>{row.types || "N/A"}</span>
                </div>
              ))}
            </div>
          ) : (
            <p className="validation-empty">No issues detected.</p>
          )}
        </section>
      </main>
    </div>
  );
}

export default StoreMapPage;
