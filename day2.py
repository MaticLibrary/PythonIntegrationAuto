import random
import logging
from typing import List, Dict

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Stałe
MAX_RETRIES = 3
API_ERROR_PROBABILITY = 0.5
NUMBER_OF_OFFERS = 400
MIN_PRICE = 10
MAX_PRICE = 500


def fetch_offers() -> List[Dict[str, int]]:
    """
    Symuluje pobieranie ofert z API.

    Returns:
        Lista słowników z danymi ofert (id, price, category).

    Raises:
        Exception: Symulacja losowego błędu API (50% szans).
    """
    if random.random() < API_ERROR_PROBABILITY:
        raise Exception("Błąd API – symulacja awarii")

    offers = []
    for offer_id in range(1, NUMBER_OF_OFFERS + 1):
        price = random.randint(MIN_PRICE, MAX_PRICE)
        category = random.choice(["A", "B", "C", "D"])
        offers.append({"id": offer_id, "price": price, "category": category})
    return offers


def fetch_with_retry(max_retries: int = MAX_RETRIES) -> List[Dict[str, int]] | None:
    """
    Próbuje pobrać oferty z ponawianiem w przypadku błędu.

    Args:
        max_retries: Maksymalna liczba prób.

    Returns:
        Lista ofert lub None, jeśli wszystkie próby zawiodły.
    """
    for attempt in range(1, max_retries + 1):
        logger.info(f"Próba {attempt}/{max_retries}")
        try:
            return fetch_offers()
        except Exception as e:
            logger.error(f"Błąd: {e}")
            if attempt == max_retries:
                logger.critical("Nie udało się pobrać ofert.")
                return None
    return None  # Zabezpieczenie – teoretycznie nieosiągalne


def main() -> None:
    """
    Główny przepływ ETL:
    1. Extract – pobranie ofert z API (z retry)
    2. Transform – przetwarzanie danych (miejsce na list comprehensions)
    3. Load – zapis / logowanie wyniku
    """
    # ==================== EXTRACT ====================
    offers = fetch_with_retry()
    if offers is None:
        return

    # Jedna linia filtruje i tworzy listę
    filtered_offers = [offer for offer in offers if offer["price"] > 48 and offer["category"] in ('A', 'B')]

    # Suma cen
    total_price = sum(offer["price"] for offer in filtered_offers)

    # Logowanie
    logger.info(f"Po transformacji: {len(filtered_offers)} ofert spełnia warunki.")
    logger.info(f"Wynik (pierwsze 5): {filtered_offers[:5]}")
    logger.info(f"Suma cen: {total_price}")

if __name__ == "__main__":
    main()
