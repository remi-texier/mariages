import functools
import time
import traceback

def exception_and_time_handler(func):
    # Un décorateur pour gérer les exceptions et mesurer le temps d'exécution d'une fonction.
    @functools.wraps(func)  # Préserve les informations de la fonction originale
    def wrapper(*args, **kwargs):
        try:
            start_time = time.time()  # Chrono pour mesurer la performance de la fonction
            result = func(*args, **kwargs)  # Exécute la fonction 
            end_time = time.time()
            print(f"Fonction {func.__name__} exécutée en {end_time - start_time:.4f} secondes")
            return result
        except Exception as e:
            # Affiche les détails de l'exception, y compris la trace de la pile
            print(f"Exception dans {func.__name__}: {e}\n{traceback.format_exc()}")
            return None
    return wrapper
