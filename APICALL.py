import pandas as pd
import yt_dlp

# Charger le fichier Excel avec les titres des musiques
input_file = 'tracks.xlsx'  # Assurez-vous que ce fichier est dans le même répertoire que ce script
output_file = 'tracks_with_youtube_links.xlsx'  # Fichier de sortie avec les liens YouTube

# Lire le fichier Excel
tracks_df = pd.read_excel(input_file)

# Fonction pour rechercher un titre sur YouTube et obtenir le lien de la première vidéo
def get_youtube_link(title):
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'format': 'best',
        'noplaylist': True,
        'extract_flat': 'in_playlist'
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            # Rechercher le titre sur YouTube
            result = ydl.extract_info(f"ytsearch:{title}", download=False)['entries'][0]
            return result['url']
        except Exception as e:
            print(f"Erreur lors de la recherche pour le titre '{title}': {e}")
            return None

# Ajouter une colonne pour les liens YouTube
tracks_df['YouTube Link'] = tracks_df['Title'].apply(get_youtube_link)

# Sauvegarder les résultats dans un nouveau fichier Excel
tracks_df.to_excel(output_file, index=False)

print(f"Les liens YouTube ont été enregistrés dans {output_file}")
