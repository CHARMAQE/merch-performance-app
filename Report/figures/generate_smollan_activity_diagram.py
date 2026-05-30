# -*- coding: utf-8 -*-
from pathlib import Path
import math

from PIL import Image, ImageDraw, ImageFont


W, H = 2800, 1450
OUT_REPORT = Path("Report/figures/architecture/smollan_activity_areas.png")
OUT_COPY = Path("Report/figures/smollan_domaines_activite.png")

BLUE = "#0A2D57"
BLUE_SOFT = "#EFF5FA"
TEXT = "#233044"
MUTED = "#667789"
CARD = "#F8FBFE"
CARD_EDGE = "#C8D5E3"
LINE = "#D8E1EA"
WHITE = "#FFFFFF"

FONT = "C:/Windows/Fonts/arial.ttf"
FONT_BOLD = "C:/Windows/Fonts/arialbd.ttf"


def font(size, bold=False):
    return ImageFont.truetype(FONT_BOLD if bold else FONT, size)


def text_size(draw, text, fnt):
    box = draw.textbbox((0, 0), text, font=fnt)
    return box[2] - box[0], box[3] - box[1]


def center_text(draw, xy, text, fnt, fill):
    x, y = xy
    tw, th = text_size(draw, text, fnt)
    draw.text((x - tw / 2, y - th / 2), text, font=fnt, fill=fill)


def rounded_rect(draw, xy, radius, fill, outline=None, width=1):
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def icon_cart(draw, x, y, s, color):
    lw = int(7 * s)
    draw.line((x, y + 10 * s, x + 24 * s, y + 10 * s), fill=color, width=lw)
    draw.line((x + 24 * s, y + 10 * s, x + 38 * s, y + 74 * s), fill=color, width=lw)
    draw.line((x + 44 * s, y + 26 * s, x + 122 * s, y + 26 * s), fill=color, width=lw)
    draw.line((x + 122 * s, y + 26 * s, x + 106 * s, y + 86 * s), fill=color, width=lw)
    draw.line((x + 50 * s, y + 86 * s, x + 106 * s, y + 86 * s), fill=color, width=lw)
    draw.line((x + 54 * s, y + 44 * s, x + 116 * s, y + 44 * s), fill=color, width=max(3, lw // 2))
    draw.line((x + 58 * s, y + 62 * s, x + 112 * s, y + 62 * s), fill=color, width=max(3, lw // 2))
    r = 10 * s
    draw.ellipse((x + 48 * s, y + 103 * s, x + 48 * s + 2 * r, y + 103 * s + 2 * r), outline=color, width=lw)
    draw.ellipse((x + 96 * s, y + 103 * s, x + 96 * s + 2 * r, y + 103 * s + 2 * r), outline=color, width=lw)


def icon_activation(draw, x, y, s, color):
    lw = int(7 * s)
    points = [(x + 18 * s, y + 58 * s), (x + 94 * s, y + 25 * s), (x + 94 * s, y + 95 * s)]
    draw.line((*points[0], *points[1]), fill=color, width=lw)
    draw.line((*points[0], *points[2]), fill=color, width=lw)
    draw.line((*points[1], *points[2]), fill=color, width=lw)
    draw.line((x + 20 * s, y + 62 * s, x + 40 * s, y + 112 * s), fill=color, width=lw)
    draw.arc((x + 108 * s, y + 36 * s, x + 150 * s, y + 86 * s), -45, 45, fill=color, width=lw)
    draw.arc((x + 116 * s, y + 20 * s, x + 178 * s, y + 102 * s), -45, 45, fill=color, width=max(3, lw // 2))
    draw.ellipse((x + 126 * s, y + 108 * s, x + 150 * s, y + 132 * s), outline=color, width=max(4, lw // 2))
    draw.arc((x + 112 * s, y + 130 * s, x + 166 * s, y + 170 * s), 200, 340, fill=color, width=max(4, lw // 2))
    draw.ellipse((x + 76 * s, y + 114 * s, x + 98 * s, y + 136 * s), outline=color, width=max(4, lw // 2))
    draw.arc((x + 62 * s, y + 136 * s, x + 112 * s, y + 172 * s), 200, 340, fill=color, width=max(4, lw // 2))


def icon_data(draw, x, y, s, color):
    lw = int(7 * s)
    w, h = 84 * s, 30 * s
    for i in range(3):
        yy = y + i * 35 * s
        draw.ellipse((x, yy, x + w, yy + h), outline=color, width=lw)
        if i < 2:
            draw.line((x, yy + h / 2, x, yy + h / 2 + 35 * s), fill=color, width=lw)
            draw.line((x + w, yy + h / 2, x + w, yy + h / 2 + 35 * s), fill=color, width=lw)
    bx = x + 115 * s
    by = y + 16 * s
    draw.line((bx, by + 96 * s, bx + 106 * s, by + 96 * s), fill=color, width=lw)
    draw.line((bx, by + 96 * s, bx, by), fill=color, width=lw)
    for xx, bh in [(16, 52), (44, 30), (72, 68)]:
        draw.rounded_rectangle(
            (bx + xx * s, by + (96 - bh) * s, bx + (xx + 16) * s, by + 96 * s),
            radius=3 * s,
            outline=color,
            width=max(4, lw // 2),
        )


def draw_connector(draw, start, end):
    draw.line((*start, *end), fill=LINE, width=7)
    for x, y in (start, end):
        draw.ellipse((x - 11, y - 11, x + 11, y + 11), fill=WHITE, outline=LINE, width=4)


def draw_arrow(draw, start, end):
    color = "#AFC0D0"
    width = 8
    draw.line((*start, *end), fill=color, width=width)

    angle = math.atan2(end[1] - start[1], end[0] - start[0])
    head_len = 34
    head_w = 22
    back_x = end[0] - head_len * math.cos(angle)
    back_y = end[1] - head_len * math.sin(angle)
    left = (
        back_x + head_w * math.sin(angle),
        back_y - head_w * math.cos(angle),
    )
    right = (
        back_x - head_w * math.sin(angle),
        back_y + head_w * math.cos(angle),
    )
    draw.polygon([end, left, right], fill=color)


def draw_smollan_circle(canvas, draw, center, radius):
    x, y = center
    draw.ellipse(
        (x - radius + 8, y - radius + 10, x + radius + 8, y + radius + 10),
        fill=(10, 45, 87, 24),
    )
    draw.ellipse(
        (x - radius, y - radius, x + radius, y + radius),
        fill=BLUE,
        outline="#D8E4EF",
        width=7,
    )

    logo = Image.open("Report/figures/Smollan_PNG.png").convert("RGBA")
    alpha = logo.getchannel("A")
    white_logo = Image.new("RGBA", logo.size, WHITE)
    white_logo.putalpha(alpha)
    target_w = int(radius * 1.62)
    target_h = int(target_w * white_logo.height / white_logo.width)
    white_logo = white_logo.resize((target_w, target_h), Image.Resampling.LANCZOS)
    canvas.alpha_composite(white_logo, (int(x - target_w / 2), int(y - target_h / 2)))


def draw_block(draw, xy, title, bullets, icon_fn, icon_scale):
    x, y, w, h = xy
    rounded_rect(draw, (x + 9, y + 10, x + w + 9, y + h + 10), 24, (10, 45, 87, 18))
    rounded_rect(draw, (x, y, x + w, y + h), 24, CARD, CARD_EDGE, 3)
    draw.rounded_rectangle((x, y, x + 18, y + h), radius=24, fill=BLUE)
    draw.rectangle((x + 12, y, x + 24, y + h), fill=BLUE)

    icon_fn(draw, x + 56, y + 64, icon_scale, BLUE)

    title_font = font(52, True)
    bullet_font = font(44)
    draw.text((x + 220, y + 64), title, font=title_font, fill=BLUE)
    draw.line((x + 220, y + 144, x + w - 48, y + 144), fill="#B8C7D6", width=5)

    by = y + 195
    for bullet in bullets:
        draw.ellipse((x + 228, by + 16, x + 245, by + 33), fill=BLUE)
        draw.text((x + 276, by), bullet, font=bullet_font, fill=TEXT)
        by += 60


img = Image.new("RGBA", (W, H), (255, 255, 255, 0))
draw = ImageDraw.Draw(img)

title = "Principaux domaines d’activité de Smollan"
center_text(draw, (W / 2, 108), title, font(78, True), BLUE)
draw.line((650, 186, W - 650, 186), fill="#E4EBF2", width=5)

center = (W // 2, 620)
center_radius = 155

left_block = (35, 330, 1080, 450)
right_block = (1685, 330, 1080, 450)
bottom_block = (860, 875, 1080, 450)

draw_block(
    draw,
    left_block,
    "Sales & Merchandising",
    [
        "Exécution en point de vente",
        "Disponibilité produit",
        "Visibilité en rayon",
        "Respect des standards merchandising",
    ],
    icon_cart,
    0.86,
)

draw_block(
    draw,
    right_block,
    "Activation & Experience",
    [
        "Activation de marque",
        "Expérience consommateur",
        "Animations commerciales",
        "Présence terrain",
    ],
    icon_activation,
    0.68,
)

draw_block(
    draw,
    bottom_block,
    "Data & Technology",
    [
        "Collecte des données terrain",
        "Reporting opérationnel",
        "Analyse de performance",
        "Aide à la décision",
    ],
    icon_data,
    0.68,
)

draw_arrow(draw, (center[0] - center_radius, center[1]), (left_block[0] + left_block[2], center[1]))
draw_arrow(draw, (center[0] + center_radius, center[1]), (right_block[0], center[1]))
draw_arrow(draw, (center[0], center[1] + center_radius), (center[0], bottom_block[1]))
draw_smollan_circle(img, draw, center, center_radius)

OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
img.save(OUT_REPORT, "PNG", dpi=(450, 450), optimize=True)
img.save(OUT_COPY, "PNG", dpi=(450, 450), optimize=True)
