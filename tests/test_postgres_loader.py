import sys
import pandas as pd
from sqlalchemy import create_engine, text
sys.path.insert(0, '../plugins')

from extractors.world_bank_extractor import WorldBankExtractor
from transformation.data_transformer import DataTransformer
from loaders.postgres_loader import PostgresLoader
from dotenv import load_dotenv
import os


load_dotenv()

# ─── Config locale ───────────────────────────────────────
POSTGRES_CONN = os.getenv("POSTGRES_CONN", "")

def test_postgres_loader():
    
    print("=" * 50)
    print("TEST 3 — PostgreSQL Loader")
    print("=" * 50)
    
    # ─────────────────────────────────────────
    # Setup — On prépare les données transformées
    # ─────────────────────────────────────────
    print("\n⏳ Préparation des données...")
    
    extractor = WorldBankExtractor(start_year=2019, end_year=2020)
    df_raw = extractor.extract_all()
    
    df_final = (
        DataTransformer(df_raw)
        .clean_nulls()
        .add_derived_metrics()
        .validate()
        .get_dataframe()
    )
    
    print(f"   ✅ {len(df_final)} enregistrements prêts à insérer")
    
    # ─────────────────────────────────────────
    # Test 3.1 — Connexion PostgreSQL
    # ─────────────────────────────────────────
    print("\n📌 Test 3.1 : Connexion PostgreSQL...")
    
    try:
        loader = PostgresLoader(conn_string=POSTGRES_CONN)
        engine = create_engine(POSTGRES_CONN)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1
        
        print("   ✅ Connexion PostgreSQL réussie")
        
    except Exception as e:
        print(f"   ❌ Connexion échouée : {e}")
        print("   💡 Vérifie ton mot de passe dans POSTGRES_CONN")
        raise
    
    # ─────────────────────────────────────────
    # Test 3.2 — Insertion simple
    # ─────────────────────────────────────────
    print("\n📌 Test 3.2 : Insertion des données...")
    
    # Nettoie la table avant le test
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE west_africa_economic_data RESTART IDENTITY"))
        conn.commit()
    print("   🧹 Table nettoyée avant insertion")
    
    # Insère les données
    loader.load(
        df=df_final,
        table_name='west_africa_economic_data',
        if_exists='append'
    )
    
    # Vérifie le nombre de lignes insérées
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM west_africa_economic_data")
        ).fetchone()[0]
    
    print(f"   ✅ {count} enregistrements insérés dans PostgreSQL")
    assert count == len(df_final), \
        f"❌ Attendu {len(df_final)} lignes, trouvé {count}"
    
    # ─────────────────────────────────────────
    # Test 3.3 — Upsert (pas de doublons)
    # ─────────────────────────────────────────
    print("\n📌 Test 3.3 : Upsert (anti-doublons)...")
    
    # On réinsère les mêmes données
    loader.upsert(df_final, 'west_africa_economic_data')
    
    with engine.connect() as conn:
        count_after_upsert = conn.execute(
            text("SELECT COUNT(*) FROM west_africa_economic_data")
        ).fetchone()[0]
    
    print(f"   📊 Lignes avant upsert : {count}")
    print(f"   📊 Lignes après upsert : {count_after_upsert}")
    assert count_after_upsert == count, \
        f"❌ Upsert a créé des doublons ! {count_after_upsert} lignes au lieu de {count}"
    print("   ✅ Upsert OK — aucun doublon créé")
    
    # ─────────────────────────────────────────
    # Test 3.4 — Vérification des données
    # ─────────────────────────────────────────
    print("\n📌 Test 3.4 : Vérification des données insérées...")
    
    with engine.connect() as conn:
        
        # Vérifie les pays
        countries = conn.execute(text(
            "SELECT DISTINCT country_name FROM west_africa_economic_data "
            "ORDER BY country_name"
        )).fetchall()
        countries = [r[0] for r in countries]
        print(f"   🌍 Pays en base : {countries}")
        assert len(countries) == 10, f"❌ Attendu 10 pays, trouvé {len(countries)}"
        print(f"   ✅ 10 pays présents")
        
        # Vérifie les années
        years = conn.execute(text(
            "SELECT DISTINCT year FROM west_africa_economic_data "
            "ORDER BY year"
        )).fetchall()
        years = [r[0] for r in years]
        print(f"   📅 Années en base : {years}")
        
        # Vérifie qu'il n'y a pas de valeurs aberrantes
        stats = conn.execute(text("""
            SELECT 
                MIN(gdp_per_capita)           AS min_gdp_per_capita,
                MAX(gdp_per_capita)           AS max_gdp_per_capita,
                MIN(economic_health_score)    AS min_health_score,
                MAX(economic_health_score)    AS max_health_score,
                COUNT(*)                      AS total_records
            FROM west_africa_economic_data
        """)).fetchone()
        
        print(f"\n   📊 Stats en base :")
        print(f"      GDP per capita  : {stats[0]:.2f} → {stats[1]:.2f}")
        print(f"      Health score    : {stats[2]:.2f} → {stats[3]:.2f}")
        print(f"      Total records   : {stats[4]}")
        
        assert stats[2] >= 0 and stats[3] <= 100, \
            "❌ Health score hors range [0-100]"
        print("   ✅ Toutes les valeurs dans les ranges attendus")
    
    # ─────────────────────────────────────────
    # Aperçu final depuis la base
    # ─────────────────────────────────────────
    print("\n📊 Aperçu depuis PostgreSQL :")
    df_check = pd.read_sql(
        "SELECT country_name, year, gdp_per_capita, "
        "economic_health_score, data_quality_flag "
        "FROM west_africa_economic_data "
        "ORDER BY country_name, year",
        con=engine
    )
    print(df_check.to_string())
    
    print("\n🎉 TEST 3 PASSED !\n")
    return True

if __name__ == "__main__":
    test_postgres_loader()