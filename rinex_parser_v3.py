#!/usr/bin/env python3
"""
RINEX Downloader — bp.eft-cors.ru
==================================
Запуск:
    pip install requests beautifulsoup4 lxml
    python rinex_parser.py
"""

import re
import sys
import time
import json
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ════════════════════════════════════════════════════════════════════════════════════════
#                           КОНФИГУРАЦИЯ
# Здесь можно задать данные аккаунта с которого мы будем парсить наши файлы,
# выбрать диапазон измерений, задать список станций с которых можно будет брать измерения
# ════════════════════════════════════════════════════════════════════════════════════════

LOGIN    = "YourLogin"
PASSWORD = "YourPassword"

BASE_URL    = "https://bp.eft-cors.ru"
OUTPUT_DIR  = Path("rinex_files")   # корневая папка; внутри будут 12-14, 12-16, 12-18
MAX_FILES   = 10                    # лимит сайта ~10 файлов/час
REQ_DELAY   = 5                     # сек между запросами
SSE_TIMEOUT = 300                   # макс. сек ожидания одного файла

# Все доступные временные диапазоны (можно добавлять свои)
ALL_TIME_RANGES = [
    {"start": "02/02/2026 12:00", "end": "02/02/2026 14:00", "tag": "12-14"},
    {"start": "02/02/2026 12:00", "end": "02/02/2026 16:00", "tag": "12-16"},
    {"start": "02/02/2026 12:00", "end": "02/02/2026 18:00", "tag": "12-18"},
    {"start": "02/03/2026 16:00", "end": "02/03/2026 18:00", "tag": "16-18"},
]

# Станции (Можно добавить свою станцию взяв код станции с сайта и прописав его в массив станций в нужном формате)
ALL_STATIONS = [
    {"code": "VOSK", "name": "Воскресенск"},
    {"code": "RUZA", "name": "Руза"},
    {"code": "SPSS", "name": "Сергиев Посад"},
    {"code": "SHAH", "name": "Шаховская"},
    {"code": "PDLK", "name": "Подольск"},
    {"code": "SHAT", "name": "Шатура"},
    {"code": "ORZU", "name": "Орехово-Зуево"},
    {"code": "LKHV", "name": "Луховицы"},
    {"code": "DMTR", "name": "Дмитров"},
    {"code": "LOBN", "name": "Лобня"},
    {"code": "NGNK", "name": "Ногинск"},
    {"code": "ODIN", "name": "Одинцово"},
    {"code": "SERP", "name": "Серпухов"},
    {"code": "TLDM", "name": "Талдом"},
    {"code": "NRFM", "name": "Наро-Фоминск"},
    {"code": "KLN2", "name": "Клин-2"},
    {"code": "EGOR", "name": "Егорьевск"},
    {"code": "ZHDR", "name": "Железнодорожный"},
    {"code": "MZSK", "name": "Можайск"},
]

# ══════════════════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════

OUTPUT_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("rinex_parser.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def safe_name(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|\s]', "_", s).strip("_")


# ══════════════════════════════════════════════════════════════════════════════
#  ИНТЕРАКТИВНЫЙ ВЫБОР
# ══════════════════════════════════════════════════════════════════════════════

def parse_indices(raw: str, max_n: int) -> list[int] | None:
    """
    Разбирает строку вида «1 2 3», «1-4», «1,3,7-9».
    Возвращает отсортированный список 1-based индексов или None при ошибке.
    """
    indices = set()
    for part in re.split(r"[,\s]+", raw.strip()):
        part = part.strip()
        if not part:
            continue
        m_range = re.match(r'^(\d+)-(\d+)$', part)
        m_num   = re.match(r'^(\d+)$', part)
        if m_range:
            a, b = int(m_range.group(1)), int(m_range.group(2))
            if a > b:
                print(f"  ⚠ Диапазон {a}-{b} некорректен")
                return None
            indices.update(range(a, b + 1))
        elif m_num:
            indices.add(int(m_num.group(1)))
        else:
            print(f"  ⚠ Не понимаю «{part}»")
            return None
    bad = [i for i in indices if i < 1 or i > max_n]
    if bad:
        print(f"  ⚠ Нет позиций: {sorted(bad)} (допустимо 1–{max_n})")
        return None
    return sorted(indices)


def choose_time_ranges() -> list[dict]:
    """Предлагает выбрать временные диапазоны."""
    print("\n" + "═" * 62)
    print("  Доступные временные диапазоны:")
    print("  " + "─" * 40)
    for i, tr in enumerate(ALL_TIME_RANGES, 1):
        print(f"  {i}.  {tr['start']}  →  {tr['end']}")
    print("  " + "─" * 40)
    print("  Enter без ввода → все диапазоны")
    print("═" * 62)

    while True:
        try:
            raw = input("  Выберите диапазоны (напр. «1 2» или «1-3»): ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit(0)

        if not raw:
            print("  → Выбраны все диапазоны")
            return list(ALL_TIME_RANGES)

        indices = parse_indices(raw, len(ALL_TIME_RANGES))
        if indices is None:
            continue

        selected = [ALL_TIME_RANGES[i - 1] for i in indices]
        print(f"  → Выбраны диапазоны: {', '.join(tr['tag'] for tr in selected)}")
        return selected


def choose_stations(time_ranges: list[dict]) -> list[dict]:
    """Предлагает выбрать станции с учётом лимита файлов."""
    files_per_station = len(time_ranges)
    max_stations = MAX_FILES // files_per_station  # сколько станций влезает в лимит

    print("\n" + "═" * 62)
    print("  Доступные станции МСК/МО:")
    print(f"  {'№':<5} {'Код':<8} Название")
    print("  " + "─" * 40)
    for i, s in enumerate(ALL_STATIONS, 1):
        print(f"  {i:<5} {s['code']:<8} {s['name']}")
    print("  " + "─" * 40)
    print(f"  Диапазонов выбрано: {files_per_station}")
    print(f"  Лимит: {MAX_FILES} файлов/час (при превышении — автокулдаун 62 мин)")
    print(f"  Enter без ввода → все станции ({len(ALL_STATIONS)} шт.)")
    print("═" * 62)

    while True:
        try:
            raw = input("  Выберите станции (напр. «1 2 3» или «1-5»): ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit(0)

        if not raw:
            selected = ALL_STATIONS[:]
            print(f"  → Выбраны все {len(selected)} станции")
            return selected

        indices = parse_indices(raw, len(ALL_STATIONS))
        if indices is None:
            continue

        selected = [ALL_STATIONS[i - 1] for i in indices]
        total    = len(selected) * files_per_station

        if total > MAX_FILES:
            print(f"\n  ⚠  {len(selected)} станций × {files_per_station} диапазона = {total} файлов")
            waves = (total + MAX_FILES - 1) // MAX_FILES
            print(f"     Будет {waves} волн с кулдауном 62 мин. Общее время: ~{waves*62} мин (~{waves*62//60}ч {waves*62%60}мин)")

            try:
                ok = input("  Продолжить? [y/n]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                sys.exit(0)
            if ok not in ("y", "д", "да", "yes", ""):
                continue

        return selected


# ══════════════════════════════════════════════════════════════════════════════
#  ЗАГРУЗЧИК
# ══════════════════════════════════════════════════════════════════════════════

class RinexDownloader:

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9",
        })
        self.downloaded = 0

    # ── Авторизация ───────────────────────────────────────────────────────────

    def auth(self) -> bool:
        log.info("── Авторизация ──────────────────────────────────────────")
        r    = self.s.get(f"{BASE_URL}/login", timeout=30)
        soup = BeautifulSoup(r.text, "lxml")

        hidden = {}
        form   = soup.find("form")
        action = "/login"
        if form:
            action = form.get("action", "/login")
            for inp in form.find_all("input", type="hidden"):
                if inp.get("name"):
                    hidden[inp["name"]] = inp.get("value", "")

        post_url = action if action.startswith("http") else BASE_URL + action

        for lf, pf in [("email", "password"), ("login", "password")]:
            pl = {**hidden, lf: LOGIN, pf: PASSWORD}
            r2 = self.s.post(post_url, data=pl, timeout=30, allow_redirects=True)
            if any(w in r2.text.lower() for w in ["выйти", "logout", "профиль", "выход"]):
                log.info("✓ Авторизация успешна")
                return True
            if "login" not in r2.url and r2.url.rstrip("/") != post_url.rstrip("/"):
                log.info("✓ Авторизация успешна (редирект)")
                return True

        log.error("✗ Авторизация не прошла")
        return False

    # ── Получаем числовой id станции из <select> ──────────────────────────────

    def fetch_station_ids(self, codes: set) -> dict:
        """
        Возвращает {code: numeric_id} для нужных кодов.
        Числовой id нужен для параметра rinex[bs].
        """
        log.info("── Получаем ID станций со страницы /rinex ───────────────")
        r = self.s.get(f"{BASE_URL}/rinex", timeout=30)
        if r.status_code != 200:
            log.error("  GET /rinex → %d", r.status_code)
            return {}

        soup    = BeautifulSoup(r.text, "lxml")
        selects = soup.find_all("select")
        if not selects:
            log.error("  <select> не найден")
            return {}

        best   = max(selects, key=lambda s: len(s.find_all("option")))
        result = {}

        for opt in best.find_all("option"):
            val  = opt.get("value", "").strip()
            text = opt.get_text(strip=True)
            if not val or val in ("0", ""):
                continue
            m = re.match(r'^.*?\[([A-Z0-9]+)\]\s*$', text)
            code = m.group(1) if m else val
            if code in codes:
                result[code] = val

        found   = list(result.keys())
        missing = [c for c in codes if c not in result]
        log.info("  Найдено: %d  |  Не найдено: %s", len(found), missing or "—")
        return result

    # ── SSE: ожидаем готовность файла ────────────────────────────────────────

    def _read_sse(self, resp: requests.Response) -> str:
        start = time.time()
        try:
            for raw_line in resp.iter_lines(decode_unicode=True):
                if time.time() - start > SSE_TIMEOUT:
                    log.warning("    Таймаут SSE")
                    break
                if not raw_line:
                    continue
                line = raw_line.strip()
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line.startswith("{"):
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "converting" in data:
                    log.info("    Файл конвертируется …")
                    continue
                fname = data.get("file_name") or data.get("filename") or data.get("file")
                if fname:
                    return str(fname)
                if data.get("result") is False:
                    log.error("    Сервер: %s", data.get("reason", data))
                    return ""
        except Exception as e:
            log.error("    Ошибка SSE: %s", e)
        return ""

    # ── Скачиваем один файл ───────────────────────────────────────────────────

    def download_one(self, bs_id: str, station: dict, tr: dict) -> bool:
        s_code = station["code"]
        s_name = station["name"]
        tag    = tr["tag"]

        log.info("  ↓  %-22s [%s]  %s", s_name, s_code, tag)

        params = {
            "email":                "",
            "rinex[bs]":            bs_id,
            "rinex[measure_start]": tr["start"],
            "rinex[measure_end]":   tr["end"],
            "rinex[timezone]":      "3",
            "rinex[version]":       "2.11",
            "rinex[frequency]":     "10",
            "send_to_email":        "false",
        }

        try:
            resp = self.s.get(
                f"{BASE_URL}/json/get-rinex",
                params=params,
                timeout=SSE_TIMEOUT,
                stream=True,
                headers={
                    "Accept":        "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Referer":       f"{BASE_URL}/rinex?basestation_id={bs_id}",
                },
            )
            resp.raise_for_status()
        except Exception as e:
            log.error("    GET /json/get-rinex: %s", e)
            return False

        log.info("    Ожидаем SSE …")
        file_name = self._read_sse(resp)
        if not file_name:
            log.warning("    ✗ file_name не получен")
            return False

        log.info("    Файл готов: %s", file_name)

        # Папка по тегу диапазона: rinex_files/12-14/
        folder = OUTPUT_DIR / tag
        folder.mkdir(exist_ok=True)

        file_url = f"{BASE_URL}/rinex/{file_name}"
        return self._save_file(file_url, file_name, folder)

    # ── Сохранение ────────────────────────────────────────────────────────────

    def _save_file(self, url: str, fname: str, folder: Path) -> bool:
        log.info("    Скачиваем: %s", url)
        try:
            r = self.s.get(
                url, timeout=120, stream=True,
                headers={"Referer": f"{BASE_URL}/rinex"},
            )
            r.raise_for_status()
        except Exception as e:
            log.error("    Ошибка скачивания: %s", e)
            return False

        path = folder / safe_name(fname)
        size = 0
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
                size += len(chunk)

        if size < 200:
            log.warning("    Файл слишком мал (%d байт)", size)
            return False

        log.info("    ✓  %s  (%.1f KB)", path.name, size / 1024)
        return True

    # ── Таймер кулдауна ───────────────────────────────────────────────────────

    def cooldown(self, minutes: int = 62):
        """Показывает обратный отсчёт покуда не истечёт кулдаун."""
        total = minutes * 60
        log.info("⏳ Кулдаун %d мин — продолжу автоматически …", minutes)
        try:
            for remaining in range(total, 0, -1):
                m, s = divmod(remaining, 60)
                # \r перезаписывает ту же строку в терминале
                print(f"\r  ⏱  Осталось до продолжения: {m:02d}:{s:02d}   ", end="", flush=True)
                time.sleep(1)
        except KeyboardInterrupt:
            print()
            log.info("Кулдаун прерван пользователем. Выход.")
            sys.exit(0)
        print()  # перенос строки после таймера
        log.info("✓ Кулдаун завершён — возобновляю скачивание\n")

    # ── Главный запуск ────────────────────────────────────────────────────────

    def run(self):
        log.info("═" * 62)
        log.info("  RINEX Downloader — bp.eft-cors.ru")
        log.info("  Лимит: %d файлов/час   Пауза: %d сек", MAX_FILES, REQ_DELAY)
        log.info("═" * 62)

        # 1. Авторизация
        if not self.auth():
            sys.exit(1)

        # 2. Выбор временных диапазонов
        time_ranges = choose_time_ranges()

        # 3. Выбор станций
        stations = choose_stations(time_ranges)

        # 4. Получаем числовые id выбранных станций
        codes  = {s["code"] for s in stations}
        id_map = self.fetch_station_ids(codes)

        stations = [s for s in stations if s["code"] in id_map]
        if not stations:
            log.error("Ни одной станции с известным ID. Выход.")
            sys.exit(1)

        # 5. Строим полную очередь задач: [(station, tr), ...]
        queue = [
            (station, tr)
            for station in stations
            for tr in time_ranges
        ]
        total_planned = len(queue)

        log.info("\n  План: %d станций × %d диапазона = %d файлов",
                 len(stations), len(time_ranges), total_planned)
        log.info("  Папки: %s",
                 "  /  ".join(str(OUTPUT_DIR / tr["tag"]) for tr in time_ranges))
        log.info("")

        # 6. Скачивание с автоматическим кулдауном
        # 6. Скачивание с автоматическим кулдауном
        ok_n           = 0
        fail_n         = 0
        batch_ok       = 0   # успешных в текущей волне
        batch_attempts = 0   # попыток в текущей волне
        task_index     = 0
        COOLDOWN_PROBE = 3   # столько неудач подряд = «уже на кд»

        while task_index < len(queue):
            station, tr = queue[task_index]
            bs_id = id_map[station["code"]]

            # Лимит исчерпан → кулдаун
            if batch_ok >= MAX_FILES:
                remaining = total_planned - task_index
                log.info(
                    "⚠  Лимит %d файлов достигнут. Осталось задач: %d / %d",
                    MAX_FILES, remaining, total_planned,
                )
                self.cooldown(62)
                batch_ok       = 0
                batch_attempts = 0

            ok = self.download_one(bs_id, station, tr)
            task_index     += 1
            batch_attempts += 1

            if ok:
                ok_n     += 1
                batch_ok += 1
                self.downloaded += 1
            else:
                fail_n += 1

            # Детектор «уже на кулдауне при старте»:
            # первые COOLDOWN_PROBE попыток — все неуспешны
            if batch_ok == 0 and batch_attempts >= COOLDOWN_PROBE:
                log.warning(
                    "\n⚠  Первые %d попыток провалились — похоже, лимит был "
                    "исчерпан ДО запуска программы.\n"
                    "   Ухожу на кулдаун 62 мин и повторю с того же места.",
                    COOLDOWN_PROBE,
                )
                self.cooldown(62)
                task_index     -= batch_attempts  # откат — повторим те же задачи
                batch_ok        = 0
                batch_attempts  = 0
                continue

            # Пауза между запросами
            if task_index < len(queue) and batch_ok < MAX_FILES:
                log.info("    Пауза %d сек …\n", REQ_DELAY)
                time.sleep(REQ_DELAY)

        # 7. Итог
        log.info("═" * 62)
        log.info("  Всё готово! Скачано: %d,  ошибок: %d", ok_n, fail_n)
        log.info("  Файлы в папках:")
        for tr in time_ranges:
            folder = OUTPUT_DIR / tr["tag"]
            files  = list(folder.glob("*")) if folder.exists() else []
            log.info("    %s/  (%d файлов)", folder, len(files))
        if fail_n:
            log.info("  Детали ошибок → rinex_parser.log")
        log.info("═" * 62)


if __name__ == "__main__":
    RinexDownloader().run()
