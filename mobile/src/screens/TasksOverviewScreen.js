import { useEffect, useState } from "react";
import { ActivityIndicator, Image, Modal, Pressable, ScrollView, Text, View } from "react-native";
import { API_BASE, getVisitTasksOverview } from "../api/backendApi";
import { colors } from "../constants/colors";
import { styles } from "../styles/appStyles";
import { formatDate, formatNumber, formatTime } from "../utils/formatters";

export function TasksOverviewScreen({
  supervisorId,
  visitId,
  storeVisit,
  merchandiser,
  onBack,
}) {
  const [overview, setOverview] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");
  const [isOsaExpanded, setIsOsaExpanded] = useState(false);
  const [isSosExpanded, setIsSosExpanded] = useState(false);
  const [selectedImage, setSelectedImage] = useState(null);

  useEffect(() => {
    let isMounted = true;

    async function loadTasksOverview() {
      if (!supervisorId || !visitId) {
        setError("Visit data is not available.");
        return;
      }

      try {
        setIsLoading(true);
        setError("");

        const result = await getVisitTasksOverview(supervisorId, visitId);

        if (isMounted) {
          setOverview(result);
        }
      } catch (loadError) {
        if (isMounted) {
          setError("Unable to load tasks overview.");
        }
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    }

    loadTasksOverview();

    return () => {
      isMounted = false;
    };
  }, [supervisorId, visitId]);

  const store = overview?.store || storeVisit || {};
  const merch = overview?.merchandiser || merchandiser || {};
  const storeCode = store?.storeCode || storeVisit?.storeCode;
  const employeeCode = merch?.employeeCode || merchandiser?.employeeCode;
  const storeFormat = store?.format || store?.storeFormat;
  const checkInOut = overview?.checkInOut || {};
  const planogramme = Array.isArray(overview?.planogramme) ? overview.planogramme : [];
  const osa = overview?.osa || {};
  const sos = overview?.sos || {};
  const quality = overview?.quality || {};
  const qualityImages = Array.isArray(quality?.images) ? quality.images : [];

  return (
    <>
      <ScrollView contentContainerStyle={styles.tasksOverviewContent}>
        <View style={styles.compactNavbar}>
          <Pressable style={styles.compactBackButton} onPress={onBack}>
            <Text style={styles.compactBackIcon}>{"‹"}</Text>
          </Pressable>
          <Text style={styles.compactNavTitle}>Tasks Overview</Text>
          <View style={styles.compactNavSpacer} />
        </View>

        <View style={styles.tasksHeaderCard}>
          <Text style={styles.heroEyebrow}>Tasks Overview</Text>
          <Text style={styles.tasksHeaderTitle} numberOfLines={2}>
            {store?.storeName || "Store visit"}
          </Text>
          {storeCode ? (
            <Text style={styles.tasksHeaderCodeText} numberOfLines={1}>
              Store code: {storeCode}
            </Text>
          ) : null}
          <Text style={styles.tasksHeaderMeta} numberOfLines={1}>
            {merch?.name || merch?.username || "Merchandiser --"}
          </Text>
          {employeeCode ? (
            <Text style={styles.tasksHeaderCodeText} numberOfLines={1}>
              Employee code: {employeeCode}
            </Text>
          ) : null}
          <Text style={styles.tasksHeaderMeta} numberOfLines={2}>
            {[formatDisplayDate(overview?.visitDate || storeVisit?.visitDate), storeFormat, store?.channel]
              .filter(isPresent)
              .join(" - ")}
          </Text>
        </View>

        {isLoading ? (
          <View style={styles.inlineState}>
            <ActivityIndicator color={colors.navy} />
            <Text style={styles.bodyText}>Loading tasks overview...</Text>
          </View>
        ) : null}

        {error ? <Text style={styles.errorText}>{error}</Text> : null}

        <SectionCard title="Check-in / Check-out">
          <View style={styles.tasksCheckTimeRow}>
            <InfoPill label="Check-in" value={formatDisplayTime(checkInOut?.checkInTime)} />
            <InfoPill label="Check-out" value={formatDisplayTime(checkInOut?.checkOutTime)} />
          </View>
          <View style={styles.tasksImagePair}>
            <ImagePreview
              label="Check-in"
              image={firstItem(checkInOut?.checkInImages)}
              onOpenImage={setSelectedImage}
            />
            <ImagePreview
              label="Check-out"
              image={firstItem(checkInOut?.checkOutImages)}
              onOpenImage={setSelectedImage}
            />
          </View>
        </SectionCard>

        <SectionCard
          title="Planogramme"
          source={overview?.planogrammeSource}
        >
          {planogramme.length > 0 ? (
            <View style={styles.tasksCardStack}>
              {planogramme.map((item, index) => (
                <PlanogrammeCard
                  key={`${item?.category || "planogramme"}-${item?.responseDate || index}`}
                  item={item}
                  onOpenImage={setSelectedImage}
                />
              ))}
            </View>
          ) : (
            <EmptyTaskState text="No planogramme data available" />
          )}
        </SectionCard>

        <ProgressTaskSection
          label="OSA"
          summary={osa}
          countText={`${formatOptionalNumber(osa?.availableCount)} / ${formatOptionalNumber(osa?.totalCount)}`}
          isExpanded={isOsaExpanded}
          onToggle={() => setIsOsaExpanded((current) => !current)}
        >
          <CategoryProgressList
            categories={osa?.categories}
            type="osa"
            emptyText="No OSA category data available"
          />
        </ProgressTaskSection>

        <ProgressTaskSection
          label="SOS"
          summary={sos}
          isExpanded={isSosExpanded}
          onToggle={() => setIsSosExpanded((current) => !current)}
        >
          <CategoryProgressList
            categories={sos?.categories}
            type="sos"
            emptyText="No SOS category data available"
          />
        </ProgressTaskSection>

        <SectionCard title="Quality" source={quality?.source}>
          {qualityImages.length > 0 ? (
            <View style={styles.tasksQualityGrid}>
              {qualityImages.map((image, index) => (
                <ImagePreview
                  key={`${image?.url || "quality"}-${image?.responseDate || index}`}
                  label={`Quality ${index + 1}`}
                  image={image}
                  compact
                  onOpenImage={setSelectedImage}
                />
              ))}
            </View>
          ) : (
            <EmptyTaskState text="No quality images available" />
          )}
        </SectionCard>
      </ScrollView>

      <ImageViewerModal
        image={selectedImage}
        onClose={() => setSelectedImage(null)}
      />
    </>
  );
}

function PlanogrammeCard({ item, onOpenImage }) {
  const singleImage = item?.singleImage || item?.image;
  const hasBeforeAfterImage = hasImageUrl(item?.beforeImage) || hasImageUrl(item?.afterImage);
  const hasSingleImage = hasImageUrl(singleImage);

  return (
    <View style={styles.tasksCategoryCard}>
      <Text style={styles.tasksCategoryTitle} numberOfLines={2}>
        {item?.category || "Uncategorized"}
      </Text>

      {hasBeforeAfterImage ? (
        <View style={styles.tasksImagePair}>
          <ImagePreview
            label="Before"
            image={item?.beforeImage}
            compact
            showDate={false}
            onOpenImage={onOpenImage}
          />
          <ImagePreview
            label="After"
            image={item?.afterImage}
            compact
            showDate={false}
            onOpenImage={onOpenImage}
          />
        </View>
      ) : hasSingleImage ? (
        <View style={styles.tasksSingleImageBlock}>
          <ImagePreview
            label="Planogramme photo"
            image={singleImage}
            compact
            showDate={false}
            onOpenImage={onOpenImage}
          />
        </View>
      ) : (
        <EmptyTaskState text="No planogramme image available" />
      )}

      {isPresent(item?.responseDate) ? (
        <Text style={styles.tasksImageMeta}>{formatDisplayDate(item.responseDate)}</Text>
      ) : null}
    </View>
  );
}

function ImageViewerModal({ image, onClose }) {
  return (
    <Modal
      animationType="fade"
      transparent
      visible={Boolean(image?.uri)}
      onRequestClose={onClose}
    >
      <View style={styles.tasksImageModalBackdrop}>
        <View style={styles.tasksImageModalHeader}>
          <Text style={styles.tasksImageModalTitle} numberOfLines={1}>
            {image?.label || "Image"}
          </Text>
          <Pressable style={styles.tasksImageModalClose} onPress={onClose}>
            <Text style={styles.tasksImageModalCloseText}>Close</Text>
          </Pressable>
        </View>

        {image?.uri ? (
          <Image
            source={{ uri: image.uri }}
            style={styles.tasksImageModalImage}
            resizeMode="contain"
          />
        ) : null}

        {isPresent(image?.responseDate) ? (
          <Text style={styles.tasksImageModalMeta}>
            {formatDisplayDate(image.responseDate)}
          </Text>
        ) : null}
      </View>
    </Modal>
  );
}

function SectionCard({ children, source, title }) {
  return (
    <View style={styles.tasksSectionCard}>
      <View style={styles.tasksSectionHeader}>
        <Text style={styles.tasksSectionTitle}>{title}</Text>
        {source ? <SourceBadge source={source} /> : null}
      </View>
      {children}
    </View>
  );
}

function ProgressTaskSection({
  children,
  countText,
  isExpanded,
  label,
  onToggle,
  summary,
}) {
  return (
    <View style={styles.tasksProgressCard}>
      <View style={styles.tasksProgressHeader}>
        <View style={styles.tasksProgressTitleBlock}>
          <Text style={styles.tasksProgressLabel}>{label}</Text>
          <SourceBadge source={summary?.source} isDark />
        </View>
        <Text style={styles.tasksProgressValue}>{formatOptionalPercentage(summary?.percentage)}</Text>
      </View>
      <View style={styles.tasksProgressTrack}>
        <View style={[styles.tasksProgressFill, { width: progressWidth(summary?.percentage) }]} />
      </View>
      {countText ? <Text style={styles.tasksProgressMeta}>{countText}</Text> : null}
      <Pressable style={styles.tasksDetailsButton} onPress={onToggle}>
        <Text style={styles.tasksDetailsButtonText}>
          {isExpanded ? "- Details by category" : "+ Details by category"}
        </Text>
      </Pressable>
      {isExpanded ? <View style={styles.tasksExpandedContent}>{children}</View> : null}
    </View>
  );
}

function CategoryProgressList({ categories, emptyText, type }) {
  const safeCategories = Array.isArray(categories) ? categories : [];

  if (safeCategories.length === 0) {
    return <EmptyTaskState text={emptyText} isDark />;
  }

  return (
    <View style={styles.tasksCategoryList}>
      {safeCategories.map((category, index) => (
        <View
          key={`${category?.category || "category"}-${index}`}
          style={styles.tasksProgressCategoryCard}
        >
          <View style={styles.tasksProgressCategoryHeader}>
            <Text style={styles.tasksProgressCategoryTitle} numberOfLines={2}>
              {category?.category || "Uncategorized"}
            </Text>
            <Text style={styles.tasksProgressCategoryValue}>
              {formatOptionalPercentage(category?.percentage)}
            </Text>
          </View>
          <View style={styles.tasksSmallProgressTrack}>
            <View style={[styles.tasksSmallProgressFill, { width: progressWidth(category?.percentage) }]} />
          </View>
          <Text style={styles.tasksProgressCategoryMeta}>
            {type === "sos"
              ? `${formatOptionalNumber(category?.value)} / ${formatOptionalNumber(category?.total)}`
              : `${formatOptionalNumber(category?.availableCount)} / ${formatOptionalNumber(category?.totalCount)}`}
          </Text>
        </View>
      ))}
    </View>
  );
}

function InfoPill({ label, value }) {
  return (
    <View style={styles.tasksInfoPill}>
      <Text style={styles.tasksInfoPillLabel}>{label}</Text>
      <Text style={styles.tasksInfoPillValue}>{value}</Text>
    </View>
  );
}

function ImagePreview({ compact = false, image, label, onOpenImage, showDate = true }) {
  const imageUri = resolveImageUri(image?.url);

  return (
    <View style={[styles.tasksImagePreview, compact ? styles.tasksImagePreviewCompact : null]}>
      <Text style={styles.tasksImageLabel}>{label}</Text>
      {imageUri ? (
        <Pressable
          accessibilityRole="imagebutton"
          style={styles.tasksImageButton}
          onPress={() => onOpenImage?.({ uri: imageUri, label, responseDate: image?.responseDate })}
        >
          <Image
            source={{ uri: imageUri }}
            style={[styles.tasksImage, compact ? styles.tasksImageCompact : null]}
            resizeMode="cover"
          />
        </Pressable>
      ) : (
        <View style={[styles.tasksImagePlaceholder, compact ? styles.tasksImageCompact : null]}>
          <Text style={styles.tasksImagePlaceholderText}>No image</Text>
        </View>
      )}
      {showDate && isPresent(image?.responseDate) ? (
        <Text style={styles.tasksImageMeta}>{formatDisplayDate(image.responseDate)}</Text>
      ) : null}
    </View>
  );
}

function SourceBadge({ isDark = false, source }) {
  const state = source?.isFallback ? "fallback" : source?.sourceVisitId ? "current" : "empty";
  const text = sourceMessage(source);

  return (
    <View
      style={[
        styles.tasksSourceBadge,
        isDark ? styles.tasksSourceBadgeDark : null,
        state === "fallback" ? styles.tasksSourceBadgeFallback : null,
        state === "empty" ? styles.tasksSourceBadgeEmpty : null,
      ]}
    >
      <Text
        style={[
          styles.tasksSourceBadgeText,
          isDark ? styles.tasksSourceBadgeTextDark : null,
          state === "fallback" ? styles.tasksSourceBadgeTextFallback : null,
          state === "empty" ? styles.tasksSourceBadgeTextEmpty : null,
        ]}
        numberOfLines={1}
      >
        {text}
      </Text>
    </View>
  );
}

function EmptyTaskState({ isDark = false, text }) {
  return (
    <Text style={[styles.tasksEmptyText, isDark ? styles.tasksEmptyTextDark : null]}>
      {text}
    </Text>
  );
}

function sourceMessage(source) {
  if (!source?.sourceVisitId) {
    return source?.message || "No data available";
  }

  if (source?.isFallback) {
    return source?.message || `Last available data: ${formatDisplayDate(source.sourceVisitDate)}`;
  }

  return source?.message || "Current visit data";
}

function resolveImageUri(url) {
  if (!isPresent(url)) {
    return null;
  }

  const value = String(url).trim().replace(/\\/g, "/");

  if (/^(https?:|data:)/i.test(value)) {
    return encodeURI(value);
  }

  if (value.startsWith("/")) {
    return encodeURI(`${API_BASE}${value}`);
  }

  return encodeURI(`${API_BASE}/${value}`);
}

function hasImageUrl(image) {
  return Boolean(resolveImageUri(image?.url));
}

function firstItem(items) {
  return Array.isArray(items) && items.length > 0 ? items[0] : null;
}

function formatDisplayDate(value) {
  const formatted = formatDate(value);
  return formatted === "N/A" ? "--" : formatted;
}

function formatDisplayTime(value) {
  const formatted = formatTime(value);
  return formatted === "N/A" ? "--" : formatted;
}

function formatOptionalPercentage(value) {
  if (!isPresent(value) || Number.isNaN(Number(value))) {
    return "--";
  }

  return `${Math.round(Number(value))}%`;
}

function formatOptionalNumber(value) {
  if (!isPresent(value) || Number.isNaN(Number(value))) {
    return "--";
  }

  return formatNumber(Number(value));
}

function progressWidth(value) {
  if (!isPresent(value) || Number.isNaN(Number(value))) {
    return "0%";
  }

  return `${Math.max(0, Math.min(100, Math.round(Number(value))))}%`;
}

function isPresent(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}
