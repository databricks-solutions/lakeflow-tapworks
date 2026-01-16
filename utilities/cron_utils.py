"""
Cron expression utilities.

Utilities for converting between different cron formats.
"""


def convert_cron_to_quartz(cron_expression: str) -> str:
    """
    Convert standard 5-field cron to Quartz 6-field cron format.

    Standard cron: minute hour day month day-of-week
    Quartz cron:   second minute hour day month day-of-week

    In Quartz cron, you must use ? for either day-of-month OR day-of-week (not both can be *).
    - If day-of-week is *, use ? for day-of-week
    - If day-of-week is specified (not *), use ? for day-of-month

    Args:
        cron_expression (str): Standard 5-field cron expression
            Example: "*/15 * * * *" or "0 9 * * 1"

    Returns:
        str: Quartz 6-field cron expression
            Example: "0 */15 * * * ?" or "0 0 9 ? * 1"

    Examples:
        >>> convert_cron_to_quartz("*/15 * * * *")
        '0 */15 * * * ?'

        >>> convert_cron_to_quartz("0 */6 * * *")
        '0 0 */6 * * ?'

        >>> convert_cron_to_quartz("0 0 * * *")
        '0 0 0 * * ?'

        >>> convert_cron_to_quartz("0 9 * * 1")
        '0 0 9 ? * 1'
    """
    parts = cron_expression.strip().split()

    if len(parts) != 5:
        # If already 6+ fields, assume it's Quartz format
        return cron_expression

    # Add seconds=0 at start
    minute, hour, day, month, dow = parts

    # In Quartz cron, you must use ? for either day-of-month OR day-of-week (not both can be *)
    # If day-of-week is *, use ? for day-of-week
    # If day-of-week is specified (not *), use ? for day-of-month
    if dow == '*':
        dow = '?'
    else:
        # day-of-week is specified, so day-of-month must be ?
        day = '?'

    quartz_cron = f"0 {minute} {hour} {day} {month} {dow}"
    return quartz_cron
