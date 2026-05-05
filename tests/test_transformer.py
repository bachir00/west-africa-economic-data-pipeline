import sys
import pandas as pd
import numpy as np
sys.path.insert(0, '../plugins')

from extractors.world_bank_extractor import WorldBankExtractor
from transformation.data_transformer import DataTransformer

def test_transformer():
    
    print("=" * 50)
    print("TEST 2 — Data Transformer")
    print("=" * 50)
    
    # On réutilise l'extracteur pour avoir des données réelles
    print("\n⏳ Récupération des données pour le test...")
    extractor = WorldBankExtractor(start_year=2019, end_year=2020)
    df_raw = extractor.extract_all()
    print(f"   ✅ {len(df_raw)} enregistrements bruts récupérés")
    
    # ─────────────────────────────────────────
    # Test 2.1 — Injection de nulls artificiels
    # pour tester le nettoyage
    # ─────────────────────────────────────────
    print("\n📌 Test 2.1 : Nettoyage des valeurs nulles...")
    
    df_with_nulls = df_raw.copy()
    df_with_nulls.loc[0, 'gdp_current_usd'] = None
    df_with_nulls.loc[1, 'inflation_rate'] = None
    df_with_nulls.loc[2, 'unemployment_rate'] = None
    
    nulls_before = df_with_nulls.isnull().sum().sum()
    print(f"   📊 Nulls avant nettoyage : {nulls_before}")
    
    transformer = DataTransformer(df_with_nulls)
    transformer.clean_nulls()
    df_clean = transformer.get_dataframe()
    
    nulls_after = df_clean[['gdp_current_usd', 'inflation_rate', 'unemployment_rate']].isnull().sum().sum()
    print(f"   📊 Nulls après nettoyage : {nulls_after}")
    assert nulls_after == 0, f"❌ Il reste encore {nulls_after} nulls !"
    print("   ✅ Tous les nulls nettoyés")
    
    # ─────────────────────────────────────────
    # Test 2.2 — Métriques dérivées
    # ─────────────────────────────────────────
    print("\n📌 Test 2.2 : Métriques dérivées...")
    
    transformer.add_derived_metrics()
    df_enriched = transformer.get_dataframe()
    
    # Vérifier que les nouvelles colonnes existent
    new_cols = ['gdp_per_capita', 'digital_adoption_score', 'economic_health_score', 'ingestion_timestamp', 'data_quality_flag']
    for col in new_cols:
        assert col in df_enriched.columns, f"❌ Colonne {col} manquante !"
        print(f"   ✅ Colonne '{col}' créée")
    
    # Vérifier que gdp_per_capita est correct
    sample = df_enriched.iloc[0]
    expected_gdp_per_capita = round(sample['gdp_current_usd'] / sample['population_total'], 2)
    assert sample['gdp_per_capita'] == expected_gdp_per_capita, \
        f"❌ gdp_per_capita incorrect ! Attendu {expected_gdp_per_capita}, obtenu {sample['gdp_per_capita']}"
    print(f"   ✅ gdp_per_capita calculé correctement")
    
    # Vérifier que economic_health_score est entre 0 et 100
    min_score = df_enriched['economic_health_score'].min()
    max_score = df_enriched['economic_health_score'].max()
    assert 0 <= min_score <= 100, f"❌ Score min hors range : {min_score}"
    assert 0 <= max_score <= 100, f"❌ Score max hors range : {max_score}"
    print(f"   ✅ economic_health_score dans range [0-100] : min={min_score}, max={max_score}")
    
    # Vérifier digital_adoption_score entre 0 et 1
    assert df_enriched['digital_adoption_score'].between(0, 1).all(), \
        "❌ digital_adoption_score hors range [0-1]"
    print(f"   ✅ digital_adoption_score dans range [0-1]")
    
    # ─────────────────────────────────────────
    # Test 2.3 — Validation
    # ─────────────────────────────────────────
    print("\n📌 Test 2.3 : Validation du DataFrame...")
    
    try:
        transformer.validate()
        print("   ✅ Validation passée")
    except AssertionError as e:
        print(f"   ❌ Validation échouée : {e}")
        raise
    
    # ─────────────────────────────────────────
    # Test 2.4 — Pipeline complet chaîné
    # ─────────────────────────────────────────
    print("\n📌 Test 2.4 : Pipeline chaîné complet...")
    
    df_final = (
        DataTransformer(df_raw)
        .clean_nulls()
        .add_derived_metrics()
        .validate()
        .get_dataframe()
    )
    
    print(f"   ✅ Pipeline chaîné OK : {len(df_final)} enregistrements")
    print(f"   📋 Colonnes finales ({len(df_final.columns)}) : {list(df_final.columns)}")
    
    # ─────────────────────────────────────────
    # Aperçu final
    # ─────────────────────────────────────────
    print("\n📊 Aperçu des données transformées :")
    cols_to_show = [
        'country_name', 'year', 
        'gdp_per_capita', 'inflation_rate',
        'economic_health_score', 'data_quality_flag'
    ]
    print(df_final[cols_to_show].to_string())
    
    print("\n🎉 TEST 2 PASSED !\n")
    return df_final

if __name__ == "__main__":
    df = test_transformer()