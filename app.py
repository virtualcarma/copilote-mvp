from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
from io import BytesIO

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des noms de colonnes
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Mapping tolérant des colonnes possibles
    column_map = {
        "date": ["date", "jour", "transaction_date"],
        "amount": ["amount", "montant", "price", "total"],
        "customer_id": ["customer_id", "client", "customer", "client_id"],
    }

    normalized = {}
    for target, variants in column_map.items():
        for v in variants:
            if v in df.columns:
                normalized[target] = v
                break

    missing = set(column_map.keys()) - set(normalized.keys())
    if missing:
        raise ValueError(
            "Colonnes non reconnues.\n\n"
            f"Colonnes trouvées dans ton CSV : {list(df.columns)}\n\n"
            "👉 Colonnes attendues (au moins une par groupe) :\n"
            "- date : date, jour, transaction_date\n"
            "- amount : amount, montant, price, total\n"
            "- customer : customer_id, client, customer, client_id\n\n"
            "✅ Astuce: renomme tes colonnes dans Excel pour matcher une des variantes ci-dessus."
        )

    # Renommage vers le schéma standard
    df = df.rename(columns={v: k for k, v in normalized.items()})

    # customer_id propre (même si numérique)
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    # Dates robustes
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Montants robustes (virgule ou point)
    df["amount"] = df["amount"].astype(str).str.replace(",", ".", regex=False)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Nettoyage final
    df = df.dropna(subset=["date", "amount", "customer_id"])
    df = df[df["amount"] != 0]

    return df


def compute_kpis_and_alerts(df: pd.DataFrame):
    daily = (
        df.groupby("date")
          .agg(
              revenue=("amount", "sum"),
              orders=("amount", "count"),
              customers=("customer_id", pd.Series.nunique),
          )
          .reset_index()
          .sort_values("date")
    )

    daily["aov"] = (daily["revenue"] / daily["orders"]).round(2)
    daily["rev_ma7"] = daily["revenue"].rolling(7, min_periods=4).mean()
    daily["rev_std7"] = daily["revenue"].rolling(7, min_periods=4).std()

    alerts = []
    if len(daily) >= 4:
        last = daily.iloc[-1]
        if pd.notna(last["rev_ma7"]) and pd.notna(last["rev_std7"]) and last["rev_std7"] > 0:
            z = (last["revenue"] - last["rev_ma7"]) / last["rev_std7"]
            if z < -2:
                alerts.append({
                    "level": "ALERTE",
                    "title": "Baisse anormale du revenu aujourd’hui",
                    "detail": f"Revenu: {last['revenue']:.2f}. Moyenne récente: {last['rev_ma7']:.2f}.",
                    "action": "Vérifie: pubs, conversion, checkout/paiements, incident produit."
                })
            elif z < -1:
                alerts.append({
                    "level": "SURVEILLANCE",
                    "title": "Revenu sous la moyenne récente",
                    "detail": f"Revenu: {last['revenue']:.2f} vs moyenne {last['rev_ma7']:.2f}.",
                    "action": "Contrôle: trafic, taux de conversion, offre, prix."
                })

    if not alerts:
        alerts.append({
            "level": "OK",
            "title": "Stabilité détectée",
            "detail": "Aucune anomalie significative sur le revenu du jour vs tendance récente.",
            "action": "Ensuite: ajouter Ads + Analytics pour CAC/ROAS."
        })

    today = daily.iloc[-1].to_dict() if len(daily) else {}
    daily_tail = daily.tail(14).copy()
    daily_tail["date"] = daily_tail["date"].astype(str)

    return daily_tail.to_dict(orient="records"), today, alerts


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload", response_class=HTMLResponse)
async def upload(request: Request, file: UploadFile = File(...)):
    content = await file.read()
    try:
        raw = BytesIO(content)

        # Détection automatique du séparateur (, ou ;)
        try:
            df = pd.read_csv(raw, sep=None, engine="python")
        except Exception:
            raw.seek(0)
            df = pd.read_csv(raw, sep=";")

        df = normalize_df(df)
        table, today, alerts = compute_kpis_and_alerts(df)

        return templates.TemplateResponse("index.html", {
            "request": request,
            "filename": file.filename,
            "alerts": alerts,
            "today": today,
            "table": table,
        })

    except Exception as e:
        return templates.TemplateResponse("index.html", {"request": request, "error": str(e)})
