from .base import Editor


class DamageChannelEditor(Editor):

    class_name = "damage_channel"

    def initially_checked(self):
        """
        Determines if the item must be initially checked. To be overriden by subclasses.
        """
        if self.obj.channel_damage_code__REL.value_en == "BCD":
            return False
        return True
