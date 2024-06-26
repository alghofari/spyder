from contextlib import contextmanager

from pyvirtualdisplay import Display

from utils.logger import Logger


class VirtualDisplayManager:
    def __init__(self, width=1920, height=1080, color_depth=24):
        self.width = width
        self.height = height
        self.color_depth = color_depth
        self.display = None
        self.logger = Logger().get_logger()
        self.logger.info(f"VirtualDisplayManager initialized with width={width}, height={height}, color_depth={color_depth}")

    def start_display(self):
        try:
            self.display = Display(visible=False, size=(self.width, self.height), color_depth=self.color_depth)
            self.display.start()
            self.logger.info("Virtual display started successfully.")
        except Exception as e:
            self.logger.error(f"Failed to start the virtual display: {e}")
            self.display = None

    def stop_display(self):
        if self.display:
            try:
                self.display.stop()
                self.logger.info("Virtual display stopped successfully.")
            except Exception as e:
                self.logger.error(f"Failed to stop the virtual display: {e}")
            finally:
                self.display = None
        else:
            self.logger.warning("Attempted to stop a virtual display that is not running.")

    @contextmanager
    def managed_display(self):
        self.start_display()
        try:
            yield
        finally:
            self.stop_display()
