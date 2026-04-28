from src.process_document import estrai_testo_da_file

file_path = "src/uploads/GN-1935689.pdf"
testo = estrai_testo_da_file(file_path)
print("Testo estratto:", testo)

# Controlla se 'Makrem' è nel testo
if "Makrem" in testo:
    print("Nome trovato nel testo!")
else:
    print("Nome NON trovato nel testo.")