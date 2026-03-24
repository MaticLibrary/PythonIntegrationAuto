import random
import logging
from typing import List, Dict

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_orders() -> List[Dict[str, int]]:
    """
    Pobiera zamówienia z API.
    
    Returns:
        Lista słowników z zamówieniami
        
    Raises:
        Exception: Jeśli API zwraca błąd (50% szansy)
    """
    if random.random() < 0.5:
        raise Exception("API error")
    return [
        {"id": 1, "price": 50},
        {"id": 2, "price": 200},
        {"id": 3, "price": 150}
    ]


def extract_with_retry(max_retries: int = 3) -> List[Dict[str, int]] | None:
    """
    Pobiera dane z retry logic.
    
    Args:
        max_retries: Maksymalna liczba prób
        
    Returns:
        Lista zamówień lub None jeśli wszystkie próby się nie powiodły
    """
    for attempt in range(1, max_retries + 1):
        logger.info(f"Próba {attempt}")
        try:
            return fetch_orders()
        except Exception as e:
            logger.error(f"Błąd: {e}")
            if attempt == max_retries:
                logger.critical("Nie udało się pobrać danych")
                return None
    return None


def transform(orders: List[Dict[str, int]]) -> List[Dict[str, int]]:
    """
    Filtruje (price > 100) i mapuje zamówienia.
    
    Args:
        orders: Lista surowych zamówień
        
    Returns:
        Przetworzene zamówienia
    """
    return [
        {"order_id": o["id"], "value": o["price"]}
        for o in orders
        if o["price"] > 100
    ]


def main() -> None:
    """Główny przepływ ETL."""
    # === EXTRACT ===
    orders = extract_with_retry(max_retries=3)
    
    if orders is None:
        return
    
    # === TRANSFORM ===
    result = transform(orders)
    
    # === LOAD ===
    logger.info(f"Przetworzono {len(result)} zamówień")
    logger.info(f"Wynik: {result}")


if __name__ == "__main__":
    main()
