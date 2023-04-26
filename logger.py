from logging import *
import logging


# log_format='{name} - {asctime} - {message} - {levelname} - {lineno}'
log_format='%(name)s - %(asctime)s - %(message)s - %(levelname)s - %(lineno)d'
formatter=logging.Formatter(log_format)
logger = getLogger("k") #--- see it ---
handler = logging.StreamHandler() #--- see it ---
handler.setLevel(logging.WARNING)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.critical("CRITICAL ISSUES HERE") # solution : It only takes one argument
print(logger.name)



'''
logger = getLogger("__main__")
basicConfig(filename='output2.log', level=DEBUG, filemode="w", style="{", format=log_format)
logger.debug("Debug")
logger.info("info")
logger.warning("warning")
logger.error("error")
logger.critical("critical")
'''

