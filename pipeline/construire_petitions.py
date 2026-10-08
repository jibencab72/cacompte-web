#!/usr/bin/env python3
"""
Pipeline « Pétitions, Ça compte » — version 2 (08/10/2026).

Télécharge les pétitions officielles (Assemblée nationale + Sénat, données
ouvertes data.gouv.fr), les normalise et publie, dans ce dépôt (GitHub Pages) :

  donnees/petitions.json        index léger (sans les textes), lu par l'appli
  donnees/textes/<id>.json      texte complet + décision officielle, à la demande
  donnees/version.json          petite fiche de version (date, empreinte, taille)

La « hausse » se calcule contre la PUBLICATION PRÉCÉDENTE (donnees/petitions.json
avant écrasement), jamais contre un fichier local qui pourrait manquer : la date de
comparaison (`hausse_depuis`) est toujours écrite à côté des chiffres.

Doctrine (héritée de Faits d'État) : chaque champ vient de la source officielle,
jamais inventé. `statut` gardé BRUT ; un `statut_code` calculé sert seulement à
l'affichage. Les seuils affichés sont les seuils OFFICIELS, jamais un jalon inventé.

Lancer : python3 pipeline/construire_petitions.py [--sortie donnees] [--assets <dossier>]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import pathlib
import sys
import time
import unicodedata
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

for _flux in (sys.stdout, sys.stderr):
    if hasattr(_flux, "reconfigure"):
        _flux.reconfigure(encoding="utf-8")

SCHEMA_VERSION = 2

# --- Sources officielles (data.gouv.fr) ---
URL_AN = "https://www.data.gouv.fr/api/1/datasets/r/c94c9dfe-23eb-45aa-acd1-7438c4e977db"
URL_SENAT = "https://www.data.gouv.fr/api/1/datasets/r/5bf276c9-1c5a-4b2a-9f02-954f297118b5"
SOURCE_AN = "https://petitions.assemblee-nationale.fr/"
SOURCE_SENAT = "https://petitions.senat.fr/"

# Seuils OFFICIELS (relus le 08/10/2026 sur petitions.senat.fr/pages/aide et le site
# de l'Assemblée) : jamais exagérés, « peut » ≠ « doit ».
SEUILS_OFFICIELS = {
    "senat": [
        {"seuil": 100_000, "delai": "6 mois",
         "declenche": "Transmission à la Conférence des présidents du Sénat, qui peut "
                      "décider d'un débat, d'une mission de contrôle, d'un renvoi en "
                      "commission… ou de ne pas donner suite."},
    ],
    "assemblee_nationale": [
        {"seuil": 100_000, "delai": "—",
         "declenche": "La pétition est publiée sur le site de l'Assemblée nationale "
                      "(visibilité accrue) ; la commission compétente peut l'examiner."},
        {"seuil": 500_000, "delai": "—",
         "declenche": "Si les signatures viennent d'au moins 30 départements ou "
                      "collectivités d'outre-mer, la Conférence des présidents PEUT "
                      "organiser un débat en séance publique."},
    ],
}

# Volumes minimaux : en dessous, la source est considérée cassée et rien n'est publié.
MIN_AN, MIN_SENAT = 3_000, 500
# Une pétition est « nouvelle » si publiée depuis moins de N jours.
JOURS_NOUVELLE = 30
# Échéance « proche » : dans les N jours.
JOURS_ECHEANCE_PROCHE = 45

UA = "PetitionsCaCompte-pipeline/2.0 (+https://jibencab72.github.io/cacompte-web/)"


def _get(url: str, essais: int = 4) -> tuple[bytes, str | None]:
    """GET avec reprises ; renvoie (contenu, Last-Modified ou None)."""
    derniere = None
    for i in range(essais):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read(), resp.headers.get("Last-Modified")
        except Exception as e:  # noqa: BLE001
            derniere = e
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"Téléchargement impossible : {url} ({derniere})")


def _csv(url: str) -> tuple[list[dict], str | None]:
    brut, lm = _get(url)
    rows = list(csv.DictReader(io.StringIO(brut.decode("utf-8")), delimiter=";"))
    return rows, lm


def _int(x) -> int:
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return 0


def _clean(x):
    if x is None:
        return None
    x = x.strip()
    return x or None


def _sans_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _statut_code(statut: str) -> str:
    s = _sans_accents(statut).lower().strip()
    if s in ("ouverte", "en cours"):
        return "ouverte"
    if s.startswith("seuil non"):
        return "seuil_non_atteint"
    if s.startswith("seuil atteint"):
        return "seuil_atteint"
    if s.startswith("class"):
        return "classee"
    if s.startswith("archiv"):
        return "archivee"
    if s.startswith("expir"):
        return "expiree"
    return "autre"


def _seuil_officiel(chambre: str, nb: int) -> int | None:
    seuils = [s["seuil"] for s in SEUILS_OFFICIELS[chambre]]
    for s in seuils:
        if nb < s:
            return s
    return None  # tous les seuils officiels sont dépassés


def _construire(rows: list[dict], chambre: str, source_url: str) -> list[dict]:
    out = []
    for r in rows:
        nb = _int(r.get("nb_votes"))
        ident = _clean(r.get("identifiant")) or _clean(r.get("url")) or r.get("titre", "")
        statut = _clean(r.get("statut")) or ""
        code = _statut_code(statut)
        seuil = _seuil_officiel(chambre, nb)
        if chambre == "senat":
            victoire = code == "seuil_atteint" or nb >= 100_000
        else:
            victoire = nb >= 100_000
        out.append({
            "id": f"{chambre[:2]}-{ident}",
            "chambre": chambre,
            "titre": _clean(r.get("titre")) or "(sans titre)",
            "nb_signatures": nb,
            "statut": statut,
            "statut_code": code,
            "date_publication": _clean(r.get("date_publication")),
            "date_echeance": _clean(r.get("date_limite_vote")),
            "echeance_generique": False,  # calculé plus bas
            "url_officielle": _clean(r.get("url")),
            "seuil_officiel": seuil,
            "restant_seuil": (seuil - nb) if seuil else 0,
            "progression": round(min(nb / seuil, 1.0), 4) if seuil else 1.0,
            "depasse_100k": nb >= 100_000,
            "depasse_500k": nb >= 500_000,
            "est_victoire": victoire,
            "en_cours": code == "ouverte",
            "a_decision": bool(_clean(r.get("decision_commission"))),
            "_texte": _clean(r.get("description")),
            "_decision": _clean(r.get("decision_commission")),
            "_commission": _clean(r.get("commission")),
            "_type": _clean(r.get("type")),
        })
    return out


def _marquer_echeance_generique(petitions: list[dict]) -> str | None:
    """À l'Assemblée, presque toutes les pétitions ouvertes portent la même date limite
    (fin de législature) : ce n'est pas une vraie échéance, on la marque pour que
    l'appli n'affiche pas « plus que N jours » à tort."""
    dates = Counter(p["date_echeance"] for p in petitions
                    if p["chambre"] == "assemblee_nationale" and p["en_cours"] and p["date_echeance"])
    if not dates:
        return None
    date, n = dates.most_common(1)[0]
    total = sum(dates.values())
    if n / total >= 0.5:
        for p in petitions:
            if p["chambre"] == "assemblee_nationale" and p["date_echeance"] == date:
                p["echeance_generique"] = True
        return date
    return None


def _charger_precedent(chemin: pathlib.Path) -> dict | None:
    if not chemin.exists():
        return None
    try:
        return json.loads(chemin.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _date_source(*last_modified: str | None, defaut: datetime) -> str:
    dates = []
    for lm in last_modified:
        if lm:
            try:
                dates.append(parsedate_to_datetime(lm).astimezone(timezone.utc))
            except Exception:  # noqa: BLE001
                pass
    return (max(dates) if dates else defaut).date().isoformat()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sortie", default=str(pathlib.Path(__file__).resolve().parent.parent / "donnees"))
    ap.add_argument("--assets", default=None, help="dossier assets de l'appli à alimenter (facultatif)")
    ap.add_argument("--an", default=None, help="CSV AN local (tests)")
    ap.add_argument("--senat", default=None, help="CSV Sénat local (tests)")
    args = ap.parse_args()
    sortie = pathlib.Path(args.sortie)
    sortie.mkdir(parents=True, exist_ok=True)

    maintenant = datetime.now(timezone.utc)
    horodatage = maintenant.isoformat(timespec="seconds")

    print("Téléchargement AN…")
    if args.an:
        an_rows, lm_an = list(csv.DictReader(open(args.an, encoding="utf-8"), delimiter=";")), None
    else:
        an_rows, lm_an = _csv(URL_AN)
    print(f"  {len(an_rows)} lignes")
    print("Téléchargement Sénat…")
    if args.senat:
        se_rows, lm_se = list(csv.DictReader(open(args.senat, encoding="utf-8"), delimiter=";")), None
    else:
        se_rows, lm_se = _csv(URL_SENAT)
    print(f"  {len(se_rows)} lignes")

    if len(an_rows) < MIN_AN or len(se_rows) < MIN_SENAT:
        print(f"ERREUR : volume anormal (AN {len(an_rows)} < {MIN_AN} ou Sénat {len(se_rows)} < {MIN_SENAT}). Rien publié.")
        return 2

    petitions = _construire(an_rows, "assemblee_nationale", SOURCE_AN) + _construire(se_rows, "senat", SOURCE_SENAT)
    ids = [p["id"] for p in petitions]
    if len(ids) != len(set(ids)):
        print("ERREUR : identifiants en double. Rien publié.")
        return 2
    date_generique = _marquer_echeance_generique(petitions)

    # --- Comparaison avec la publication précédente ---
    chemin_index = sortie / "petitions.json"
    precedent = _charger_precedent(chemin_index)
    anciens = {}
    hausse_depuis = None
    if precedent and isinstance(precedent.get("petitions"), list):
        anciens = {p.get("id"): p for p in precedent["petitions"] if p.get("id")}
        hausse_depuis = precedent.get("donnees_au") or (precedent.get("genere_le") or "")[:10] or None
        if len(anciens) and len(petitions) < 0.9 * len(anciens):
            print(f"ERREUR : {len(petitions)} pétitions contre {len(anciens)} publiées avant (< 90 %). Rien publié.")
            return 2

    aujourdhui = maintenant.date()
    evenements = []
    nb_nouvelles = nb_hausse = 0
    for p in petitions:
        anc = anciens.get(p["id"])
        pub = p["date_publication"]
        try:
            recente = pub is not None and (aujourdhui - datetime.fromisoformat(pub[:10]).date()).days <= JOURS_NOUVELLE
        except ValueError:
            recente = False
        p["nouvelle"] = bool(recente and (anc is None or not anciens))
        if p["nouvelle"]:
            nb_nouvelles += 1
        if anc is not None:
            p["hausse"] = p["nb_signatures"] - int(anc.get("nb_signatures", 0))
            if p["hausse"] > 0:
                nb_hausse += 1
            ancien_nb = int(anc.get("nb_signatures", 0))
            for seuil in (100_000, 500_000):
                if ancien_nb < seuil <= p["nb_signatures"]:
                    evenements.append({"type": "seuil_franchi", "id": p["id"], "titre": p["titre"],
                                       "chambre": p["chambre"], "seuil": seuil, "nb_signatures": p["nb_signatures"]})
            if not anc.get("est_victoire") and p["est_victoire"] and p["chambre"] == "senat":
                evenements.append({"type": "seuil_atteint_senat", "id": p["id"], "titre": p["titre"],
                                   "chambre": p["chambre"], "nb_signatures": p["nb_signatures"]})
        else:
            p["hausse"] = None
        # échéance proche (vraie date, non générique)
        proche = False
        if p["en_cours"] and p["date_echeance"] and not p["echeance_generique"]:
            try:
                d = datetime.fromisoformat(p["date_echeance"][:10]).date()
                proche = 0 <= (d - aujourdhui).days <= JOURS_ECHEANCE_PROCHE
            except ValueError:
                pass
        p["echeance_proche"] = proche

    donnees_au = _date_source(lm_an, lm_se, defaut=maintenant)

    # --- Index publié (sans textes) ---
    index = []
    for p in petitions:
        q = {k: v for k, v in p.items() if not k.startswith("_")}
        index.append(q)
    index.sort(key=lambda p: (p["chambre"], p["id"]))
    stats = {
        "nb_total": len(index),
        "nb_en_cours": sum(1 for p in index if p["en_cours"]),
        "nb_nouvelles": nb_nouvelles,
        "nb_en_hausse": nb_hausse,
        "nb_victoires": sum(1 for p in index if p["est_victoire"]),
        "nb_seuil_franchi_depuis": len(evenements),
    }
    sortie_index = {
        "schema_version": SCHEMA_VERSION,
        "genere_le": horodatage,
        "donnees_au": donnees_au,
        "hausse_depuis": hausse_depuis,
        "echeance_generique_an": date_generique,
        "sources": {"assemblee_nationale": SOURCE_AN, "senat": SOURCE_SENAT,
                    "data_gouv_an": URL_AN, "data_gouv_senat": URL_SENAT},
        "seuils_officiels": SEUILS_OFFICIELS,
        "stats": stats,
        "evenements": evenements,
        "nb_petitions": len(index),
        "petitions": index,
    }
    texte_index = json.dumps(sortie_index, ensure_ascii=False, separators=(",", ":"))

    # Textes : un fichier par pétition (à la demande dans l'appli).
    dossier_textes = sortie / "textes"
    dossier_textes.mkdir(parents=True, exist_ok=True)
    existants = {f.name for f in dossier_textes.glob("*.json")}
    ecrits = 0
    for p in petitions:
        contenu = {"id": p["id"], "texte": p["_texte"] or "", "decision": p["_decision"],
                   "commission": p["_commission"], "type": p["_type"]}
        data = json.dumps(contenu, ensure_ascii=False, separators=(",", ":"))
        f = dossier_textes / f"{p['id']}.json"
        if not f.exists() or f.read_text(encoding="utf-8") != data:
            f.write_text(data, encoding="utf-8")
            ecrits += 1
        existants.discard(f.name)
    for orphelin in existants:  # pétition disparue de la source : on garde le texte (lien mort évité), rien à faire
        pass

    chemin_index.write_text(texte_index, encoding="utf-8")
    empreinte = hashlib.sha256(texte_index.encode("utf-8")).hexdigest()
    version = {
        "schema_version": SCHEMA_VERSION,
        "genere_le": horodatage,
        "donnees_au": donnees_au,
        "nb_petitions": len(index),
        "sha256": empreinte,
        "taille_octets": len(texte_index.encode("utf-8")),
        "fichier": "petitions.json",
    }
    (sortie / "version.json").write_text(json.dumps(version, ensure_ascii=False, indent=1), encoding="utf-8")

    # Assets de l'appli (facultatif) : index + textes des pétitions en cours ou victorieuses.
    if args.assets:
        assets = pathlib.Path(args.assets)
        assets.mkdir(parents=True, exist_ok=True)
        (assets / "petitions.json").write_text(texte_index, encoding="utf-8")
        embarques = {p["id"]: {"texte": p["_texte"] or "", "decision": p["_decision"]}
                     for p in petitions if p["en_cours"] or p["est_victoire"]}
        (assets / "petitions_textes.json").write_text(
            json.dumps(embarques, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"Assets : index + {len(embarques)} textes embarqués → {assets}")

    print("\n===== RAPPORT =====")
    print(f"Total : {len(index)} pétitions ({len(an_rows)} AN + {len(se_rows)} Sénat), données au {donnees_au}")
    print(f"En cours : {stats['nb_en_cours']} · nouvelles (≤ {JOURS_NOUVELLE} j) : {nb_nouvelles} · en hausse : {nb_hausse} "
          f"(depuis {hausse_depuis or 'aucune publication précédente'})")
    print(f"Victoires : {stats['nb_victoires']} · seuils franchis depuis la dernière publication : {len(evenements)}")
    print(f"Échéance générique AN : {date_generique} · textes écrits/modifiés : {ecrits}")
    print(f"Index : {len(texte_index)/1e6:.2f} Mo, sha256 {empreinte[:12]}…")
    return 0


if __name__ == "__main__":
    sys.exit(main())
