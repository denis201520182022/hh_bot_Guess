# app/core/exceptions.py

class DialogueLockedError(Exception):
    """Исключение, выбрасываемое когда диалог уже занят другим воркером."""
    pass
