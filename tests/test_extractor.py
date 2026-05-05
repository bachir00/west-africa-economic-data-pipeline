import sys
sys.path.insert(0, '../plugins')

from extractors.world_bank_extractor import WorldBankExtractor

def test_extraction():
    
    print("=" * 50)
    print("TEST 1 — World Bank Extractor")
    print("=" * 50)
    
    extractor = WorldBankExtractor(
        start_year=2019,  # Petit range pour tester vite
        end_year=2020
    )
    
    # Test 1.1 — Un seul pays, un seul indicateur
    print("\n📌 Test 1.1 : Fetch un indicateur pour le Sénégal...")
    records = extractor.fetch_indicator(
        country_code='SN',
        indicator='NY.GDP.MKTP.CD'
    )
    
    print(f"   ✅ Résultat : {len(records)} enregistrements reçus")
    print(f"   📄 Exemple : {records[0] if records else 'Aucun'}")
    
    # Test 1.2 — Extraction complète tous pays
    print("\n📌 Test 1.2 : Extraction complète (10 pays, 5 indicateurs)...")
    df = extractor.extract_all()
    
    print(f"   ✅ DataFrame shape : {df.shape}")
    print(f"   📋 Colonnes : {list(df.columns)}")
    print(f"   🌍 Pays : {df['country_name'].unique().tolist()}")
    print(f"   📅 Années : {sorted(df['year'].unique().tolist())}")
    
    # Test 1.3 — Vérifications basiques
    print("\n📌 Test 1.3 : Vérifications qualité...")
    
    assert len(df) > 0, "❌ DataFrame vide !"
    print("   ✅ DataFrame non vide")
    
    assert 'country_code' in df.columns, "❌ Colonne country_code manquante"
    print("   ✅ Colonne country_code présente")
    
    assert 'year' in df.columns, "❌ Colonne year manquante"
    print("   ✅ Colonne year présente")
    
    assert df['country_code'].nunique() == 10, \
        f"❌ Attendu 10 pays, obtenu {df['country_code'].nunique()}"
    print(f"   ✅ 10 pays extraits")
    
    # Aperçu des données
    print("\n📊 Aperçu des données extraites :")
    print(df.head(5).to_string())
    
    print("\n🎉 TEST 1 PASSED !\n")
    return df

if __name__ == "__main__":
    df = test_extraction()