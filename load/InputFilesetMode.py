# InputFilesetMode.py
class InputFilesetMode:

	MODE_AUDIO_ID = 3
	MODE_VIDEO_ID = 5
	MODE_TEXT_ID = 1

	@staticmethod
	def mode_id(type_code):
		if type_code == "video":
			return 	InputFilesetMode.MODE_VIDEO_ID
		elif type_code == "audio":
			return 	InputFilesetMode.MODE_AUDIO_ID
		elif type_code == "text":
			return InputFilesetMode.MODE_TEXT_ID
		else:
			return None
