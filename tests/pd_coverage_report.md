# Report: applicazione dati di copertura da pd25/pd26 ai teaching-*.yml

Pipeline: `tests/pd25.csv` + `tests/pd26.csv` -> `scraping/merge_pd25_pd26.py` -> `tests/pd25_pd26_merged.csv` -> `scraping/apply_pd_coverage.py --apply` -> blocco `coverage:` appeso ai file `data/courses/<docente>/2025/teaching-<id>.yml`.

## Numeri finali

| | Righe |
|---|---|
| Righe totali in `pd25_pd26_merged.csv` | 452 |
| Risolte (matricola -> email -> cartella -> corso -> modulo -> teaching_id) | **313** |
| File `teaching-*.yml` effettivamente scritti | **305** |
| Righe risolte ma il file era già coperto (skip, vedi sotto) | 8 |
| Non risolte | 139 |

Ogni file scritto ha ricevuto in coda un blocco:
```yaml
coverage:
  cod_copertura: '01'
  desc_copertura: Copertura per titolarita/responsabilita...
  gratuito_retribuito: GRATUITO
  ore_frontali_erogato: 30
  ore_contratto_erogato: 0
  ore_frontali_erogato_molt: 30.0
  cfu_erogati: 4
  cfu_erogati_molt: 4
```

## Attenzione: 2 conflitti risolti "silenziosamente" (primo che arriva vince)

Lo script scrive il blocco `coverage` una sola volta per file (se il file lo ha già, salta). Su 8 righe risolte che puntavano a un file già coperto, **6 sono duplicati innocui** (stessa riga pd25 ripetuta con piccola variazione di formato, es. matricola `030905` vs `30905`, CSR `'-` vs `-` — stesso dato, nessuna perdita). **2 sono conflitti reali**, dove due righe diverse con valori diversi puntavano allo stesso file, e solo la prima incontrata è stata scritta:

| File | Riga scritta | Riga scartata |
|---|---|---|
| `data/courses/matteo.ferrara/2025/teaching-455807.yml` | cod Materia 91250, ore=54, CFU=6 (da pd25) | cod Materia C8836 (stesso insegnamento, cod_integrato 91250), ore=**60**, CFU=6 (da pd26) |
| `data/courses/federico.montori/2025/teaching-367016.yml` | modulo 1, ore=16, CFU=2 | modulo 2, ore=**40**, CFU=4 -- il docente ha *un solo* modulo scrapato (`details: ['6 cfu']`, nessun numero), quindi entrambi i moduli reali del CSV cadono sullo stesso (unico) file |

Questi due vanno controllati a mano: il Ferrara è probabilmente un aggiornamento pd26 (60 è il dato più recente); il Montori è un limite dei dati scrapati (mancano due file `teaching-*.yml` distinti per i suoi due moduli, ne esiste solo uno).

## Non risolte: 139, tutte per motivi verificati (report completo in `tests/pd_coverage_unresolved.csv`)

| Motivo | Righe | Perché |
|---|---|---|
| `no_contact` | 65 | Matricola non in `data/contacts.csv`. 63 sono matricola placeholder `000000`/`0` (`n.d.`, docente non ancora assegnato nella fonte). 2 sono matricole reali (`ZINGARO`, `CICCARESE`) semplicemente non presenti nel contatti. |
| `no_course_file` | 54 | Cartella docente trovata, ma nessun `course-<cod_integrato/cod Materia>*.yml` corrispondente: sono insegnamenti/percorsi fuori dal perimetro scrapato in `data/courses` (es. corsi di formazione insegnanti "A041" di Davoli/Martini, non corsi di laurea in Informatica). |
| `no_module_match` | 12 | File e docente trovati, ma il modulo scrapato ha un numero diverso da quello richiesto dalla riga (es. MALIZIA: la riga chiede "modulo 1", nei dati scrapati c'è solo "Module 2"). Verificato uno per uno: sempre lo stesso pattern, un solo modulo scrapato col numero sbagliato -- gap dello scraping, non un bug di matching. |
| `no_teacher_folder` | 6 | Email trovata in contacts.csv ma nessuna cartella `data/courses/<username>/2025/` (es. `paolo.ciancarini`, `sofia.avnet`, `nicolo.romandini` -- non scrapati). |
| `ambiguous_module_match` | 2 | Solo Asperti (cod 91250/B2127, DEEP LEARNING): 3 moduli candidati, nessuno con CSR o numero-modulo indicato né nel CSV né nei `details` scrapati -- impossibile scegliere senza indovinare. |

## Bug corretti durante lo sviluppo (per traccia)

1. **Token modulo "nudo"**: alcuni `details` scrapati usano `'1'` invece di `'Module 1'`; la regex iniziale non lo riconosceva e lo trattava come un falso tag CSR in conflitto (es. PERONI/HEIBI, cod 75969, CSR `A-L`/`M-Z`). Corretto: 2 righe recuperate.
2. **Punteggio troppo permissivo**: la logica iniziale considerava "confermato" un modulo se una sola tra CSR e numero-modulo corrispondeva, anche quando un altro modulo candidato corrispondeva su entrambe. Passato a un punteggio (ogni conferma +1, un tag CL.x inatteso quando il CSV dice "-" è -1, contraddizione esplicita esclude il candidato). Corretto: risolti i casi Girau/Salomoni (cod 00819) e ridotti gli ambigui da 5 a 2 (i 2 rimasti sono genuinamente irrisolvibili, non un bug).
3. **`cod Materia` vs `cod_integrato`**: per i "corsi integrati" (C.I.), `cod Materia` è il codice del contenitore mentre i file `course-*.yml` sono nominati secondo `cod_integrato` (il codice della disciplina componente). Passato a provare entrambi i codici: +32 righe risolte (da 278 a 310).

## File prodotti

- `scraping/merge_pd25_pd26.py` (+ wrapper `merge_pd25_pd26.py`): merge pd25.csv/pd26.csv -> `tests/pd25_pd26_merged.csv`
- `scraping/apply_pd_coverage.py` (+ wrapper `apply_pd_coverage.py`): applica il merge ai `teaching-*.yml` (`--apply` per scrivere, senza è dry-run)
- `tests/pd25_pd26_merged.csv`: le 452 righe unite
- `tests/pd_coverage_unresolved.csv`: le 139 righe non risolte con motivo e dettaglio
- 305 file `data/courses/<docente>/2025/teaching-*.yml` aggiornati con il blocco `coverage:`

Lo script è idempotente: rilanciarlo con `--apply` non duplica i blocchi già scritti (li salta), quindi è sicuro correggere i 2 conflitti sopra a mano e rilanciare in futuro se necessario.
