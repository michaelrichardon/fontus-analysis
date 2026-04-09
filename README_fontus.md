# Fontus Jira Analyse

Automatisierte Bug-Analyse und Statuswechsel-Auswertung für das Projekt Fontus.
Das Script lädt alle Bugs aus Jira, analysiert Statuswechsel und Liegezeiten
und exportiert die Ergebnisse als Excel, JSON und SQLite.

## Was das Script macht

- Lädt alle Bugs aus Jira via REST API (gefiltert nach Feature Teams)
- Erkennt das technische Custom Field für "Feature Team" automatisch
- Berechnet Verweildauer je Status und Kategorie (Development, Test, Blocked, ...)
- Zählt Retest-Zyklen je Bug
- Wertet Root Cause und Komponenten aus
- Exportiert als Excel (7 Tabs), JSON und SQLite (7 Tabellen)
- SQLite-DB wächst mit jedem Lauf – ermöglicht Trendauswertung in Power BI

## Ausgaben

| Datei | Inhalt |
|---|---|
| `fontus_analysis_YYYYMMDD_HHMM.xlsx` | Excel mit 7 Tabs |
| `fontus_transitions_YYYYMMDD_HHMM.json` | Alle Ergebnisse als JSON |
| `fontus_analysis.db` | SQLite-Datenbank (kumulativ) |

### Excel-Tabs

1. Defects – Stammdaten aller Bugs
2. Statuswechsel – Jedes Status-Segment je Bug
3. Retest-Matrix – Retest-Anzahl je Bug
4. Liegezeiten – Aggregiert nach Team × Kategorie × Priorität
5. Root Cause – Häufigkeit je Root Cause (nur Done/Closed)
6. Komponenten – Betroffene Defects je Komponente
7. Charts – Grafische Darstellungen

### SQLite-Tabellen

| Tabelle | Inhalt |
|---|---|
| `runs` | Ein Eintrag pro Analyselauf |
| `defects` | Stammdaten je Bug |
| `segments` | Status-Segmente je Bug |
| `retest_matrix` | Retest-Anzahl je Bug |
| `dwell_times` | Aggregierte Liegezeiten |
| `root_cause` | Root-Cause-Häufigkeiten |
| `components` | Komponenten-Häufigkeiten |

## Voraussetzungen

- Python 3.11+
- Pakete: siehe `requirements.txt`
- Zugang zur Jira-Instanz (Self-hosted, Server/DC)

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

## Konfiguration

Kopiere `.env.example` zu `.env` und trage die Werte ein:

```
JIRA_BASE_URL=https://jira.local.wmgruppe.de
JIRA_PAT=DEIN_PERSONAL_ACCESS_TOKEN
JIRA_PROJECT=FONTUS
JIRA_SSL_VERIFY=false
JIRA_FEATURE_TEAM_FIELD=customfield_11101
JIRA_DB_PATH=fontus_analysis.db
```

Das Feld `JIRA_FEATURE_TEAM_FIELD` wird beim ersten Lauf automatisch ermittelt
und in der Konsole ausgegeben – danach hier eintragen um die Erkennung zu überspringen.

## Ausführung

```bash
python jira_analysis.py
```

## Jenkins

Automatischer Lauf jeden Montag um 06:00 Uhr.

- Jenkinsfile: `Jenkinsfile.txt`
- Artefakte (Excel, JSON) werden als Build-Artefakte archiviert
- SQLite-DB wird unter `C:\Jenkins\fontus\fontus_analysis.db` kumuliert
- API-Token wird als Jenkins Credential `jira-pat-fontus` hinterlegt

## Abgeschlossen / Offen / Rejected

Bugs werden in der Spalte "Abgeschlossen" mit drei Werten gekennzeichnet:

| Wert | Bedeutung |
|---|---|
| `Ja` | Status Done oder Closed |
| `Nein` | Noch offen |
| `reject` | Status Rejected |

## Konfigurierbare Konstanten im Script

| Konstante | Bedeutung |
|---|---|
| `FEATURE_TEAMS` | Liste der auszuwertenden Teams |
| `TEAM_MAPPING` | Aggregierung von Teams zu Gruppen |
| `STATUS_CATEGORY_MAPPING` | Zuordnung Status → Kategorie |
| `CLOSED_STATUSES` | Status die als abgeschlossen gelten |
| `REJECTED_STATUSES` | Status die als rejected gelten |
| `EXCLUDE_LABELS` | Labels die Issues ausschließen |
| `RELEASE_LABELS` | Labels die als Release-Werte interpretiert werden |
| `RETEST_STATUS` | Status dessen Eintritte als Retest gezählt werden |
