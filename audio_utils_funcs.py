from moviepy.editor import *
from pydub import AudioSegment
from pydub.playback import play
from azure.cognitiveservices.speech import AudioDataStream, SpeechConfig, SpeechSynthesizer, SpeechSynthesisOutputFormat
from azure.cognitiveservices.speech.audio import AudioOutputConfig
import os

import time
#from rev_ai import apiclient

def generate_azure_voice_name_dict():
    """
    Create a dictionary of all the available azure voicenames
    - Then for the tts, all you need to do is pass the name, and it would pull out the voicename and style within the 
    - ** Save this to s3 later and read it in from there
    """
    azure_voice_name_dict = {'Natasha': {'voicename': 'en-AU-NatashaNeural',
                 'style': 'General',
                 'country': 'AU'},
     'William': {'voice_name': 'en-AU-WilliamNeural',
                 'style': 'General',
                 'country': 'AU'},
     'Clara': {'voice_name': 'en-CA-ClaraNeural',
               'style': 'General',
               'country': 'CA'},
     'Liam': {'voice_name': 'en-CA-LiamNeural',
              'style': 'General',
              'country': 'CA'},
     'Neerja': {'voice_name': 'en-IN-NeerjaNeural',
                'style': 'General',
                'country': 'IN'},
     'Prabhat': {'voice_name': 'en-IN-PrabhatNeural',
                'style': 'General',
                'country': 'IN'},
     'Emily': {'voice_name': 'en-IE-EmilyNeural',
               'style': 'General',
               'country': 'IE'},
     'Connor': {'voice_name': 'en-IE-ConnorNeural', 
                'style': 'General',
                'country': 'IE'},
     'Rosa': {'voice_name': 'en-PH-RosaNeural',
              'style': 'General',
              'country': 'PH'},
     'James': {'voice_name': 'en-PH-JamesNeural',
               'style': 'General',
               'country': 'PH'},
     'Libby': {'voice_name': 'en-GB-LibbyNeural',
               'style': 'General',
               'country': 'GB'},
     'Ryan': {'voice_name': 'en-GB-RyanNeural',
              'style': 'General',
              'country': 'GB'},
     'Mia': {'voice_name': 'en-GB-MiaNeural',
             'style': 'General',
             'country': 'GB'},
     'Aria1': {'voice_name': 'en-US-AriaNeural',
              'style': 'newscast-formal',
              'country': 'US'},
     'Aria2': {'voice_name': 'en-US-AriaNeural',
              'style': 'newscast-casual',
              'country': 'US'},
     'Aria3': {'voice_name': 'en-US-AriaNeural',
              'style': 'narration-professional',
              'country': 'US'},
     'Aria4': {'voice_name': 'en-US-AriaNeural',
              'style': 'customerservice',
              'country': 'US'},
     'Aria5': {'voice_name': 'en-US-AriaNeural',
              'style': 'chat',
              'country': 'US'},
     'Aria6': {'voice_name': 'en-US-AriaNeural',
              'style': 'cheerful',
              'country': 'US'},
     'Aria7': {'voice_name': 'en-US-AriaNeural',
              'style': 'empathetic',
              'country': 'US'},
     'Jenny1': {'voice_name': 'en-US-JennyNeural',
                'style': 'customerservice',
                'country': 'US'},
     'Jenny2': {'voice_name': 'en-US-JennyNeural',
                'style': 'chat',
                'country': 'US'},
     'Jenny3': {'voice_name': 'en-US-JennyNeural',
                'style': 'assistant',
                'country': 'US'},
     'Jenny4': {'voice_name': 'en-US-JennyNeural',
                'style': 'newscast',
                'country': 'US'},
     'Guy1': {'voice_name': 'en-US-GuyNeural',
             'style': 'General',
             'country': 'US'},
     'Guy2': {'voice_name': 'en-US-GuyNeural',
             'style': 'newscast',
             'country': 'US'}}

    return azure_voice_name_dict


def azure_text_to_speech(voicename, voice_name_dict, text, key, output_filename):
    # Initialise the speech params
    voice_params_dict = voice_name_dict[voicename]
    speaker = voice_params_dict['voice_name']
    style = voice_params_dict['style']
    
    speech_config = SpeechConfig(subscription=key, region="eastus")
    try:
        
        synthesizer = SpeechSynthesizer(speech_config=speech_config, audio_config=None)
        ssml_string = '''<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis"
               xmlns:mstts="https://www.w3.org/2001/mstts" xml:lang="en-US">
            
            <voice name="%s">
                <mstts:express-as style="%s">
                    %s
                </mstts:express-as>
            </voice>
        </speak>''' % (speaker, style, text)

        result = synthesizer.speak_ssml_async(ssml_string).get()
        stream = AudioDataStream(result)
        file_path = stream.save_to_wav_file(output_filename)
        audio = output_filename
        sound = AudioSegment.from_file(output_filename, format="mp3")
#         play(sound)
    except Exception as e:
        print(e)
        audio = 'Unable to convert the text'
    return audio  


# <speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-US">
#     <voice name="en-US-GuyNeural">
#         <prosody rate="+30.00%">
#             Welcome to Microsoft Cognitive Services Text-to-Speech API.
#         </prosody>
#     </voice>
# </speak>
#<prosody rate="+30.00%">%s</prosody>
#<mstts:express-as style="%s">
#</mstts:express-as>

    #'''speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-US">
    #<voice name="en-US-GuyNeural">
        #<prosody rate="+30.00%">
            #Welcome to Microsoft Cognitive Services Text-to-Speech API.
        #</prosody>
    #</voice>
#</speak>'''


# <speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-US">
#     <voice name="en-US-GuyNeural">
#         <prosody rate="+30.00%">
#             Welcome to Microsoft Cognitive Services Text-to-Speech API.
#         </prosody>
#     </voice>
# </speak>


def azure_text_to_speech_faster(voicename, voice_name_dict, text, key, output_filename):
    # Initialise the speech params
    voice_params_dict = voice_name_dict[voicename]
    speaker = voice_params_dict['voice_name']
    style = voice_params_dict['style']
    
    speech_config = SpeechConfig(subscription=key, region="eastus")
    try:
        
        synthesizer = SpeechSynthesizer(speech_config=speech_config, audio_config=None)
        ssml_string = '''<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xmlns:mstts="https://www.w3.org/2001/mstts" xml:lang="en-US">
            <voice name="%s">
                <mstts:express-as style="%s">
                    <prosody rate="+15.00%%">%s</prosody> 
                </mstts:express-as>
            </voice>
        </speak>''' % (speaker, style, text)

        result = synthesizer.speak_ssml_async(ssml_string).get()
        stream = AudioDataStream(result)
        file_path = stream.save_to_wav_file(output_filename)
        audio = output_filename
        sound = AudioSegment.from_file(output_filename, format="mp3")
#         play(sound)
    except Exception as e:
        print(e)
        audio = 'Unable to convert the text'
    return audio  


def extract_mp3_audio_from_mp4_file(video_path, output_audio_path):
    """
    This function takes in an input 
    """
    audio = AudioSegment.from_file(video_path, format="mp4")
    audio.export(output_audio_path, format="mp3")
    
    return None


def match_target_amplitude(sound, target_dBFS):
    """
    This function takes a target_dbFS (loudness) and matches any input
    audio to that target
    """
    change_in_dBFS = target_dBFS - sound.dBFS
    return sound.apply_gain(change_in_dBFS)


def add_padding_to_clip(audio_clip, padding=1):
    """
    Round audio to the nearest second and add a second
    """
    audio_len = len(audio_clip)
    rounded = int(audio_len/1000) + padding # based on the number of gaps we're seeing, we may decide to change to +1 instead of +2
    diff = (rounded*1000) - audio_len
    extra_sec_segment = AudioSegment.silent(diff)
    audio_clip = audio_clip + extra_sec_segment
    return audio_clip


def generate_typing_effect_audio_clip(FPS, typing_audio_path, num_frames):
    """
    This generates an audio clip that adds a typing audio sequence to the story animation
    """
    # We want to generate audio that lasts up until the text stops appearing, and then add silent sound afterwards
    # We calculate how long it takes for the story text to appear across the screen
    text_seconds = round(num_frames/FPS,2)

    # Read in the typing audio
    typing_audio_full = AudioSegment.from_mp3(typing_audio_path)
    text_typing_audio = typing_audio_full[500:(text_seconds*1000) + 500] # This aims to avoid the situation where theres a gap

    return text_typing_audio


def transcribe_audio_file(audio_path, rev_ai_key, time_lim = 12):
    """
    This function takes in an input audiofile and then sends to rev.ai for transcription
    ** - This will to be a parallel process when putting in production to save you time
    * - Only do the audio of the subclip in the case where it is concatenated to 15 seconds
    """
    # Initialise the Rev AI client
    client = apiclient.RevAiAPIClient(rev_ai_key)

    # Pass the local file into the function
    job = client.submit_job_local_file(audio_path)

    # Get the job ID
    job_id = job.id

    # Pass the audio file to rev AI until the transcription is complete
    # Its currently setup to run for a maximum of 3 minutes
    for i in range(time_lim):
        job_details = client.get_job_details(job_id)
        curr_status = str(job_details.status)
        if 'IN_PROGRESS' in curr_status:
            pass
        elif 'TRANSCRIBED' in curr_status:
            break
        else:
            print(curr_status)
            print('Unknown Status')
        time.sleep(10)

    # Get the transcription as a JSON
    transcript_json = client.get_transcript_json(job_id)

    # Parse through the transcript_json and pull out the transcripted text
    text_list = []
    for speaker_dict in transcript_json['monologues']:
        for text_dict in speaker_dict['elements']:
            if text_dict['type'] == 'text':
                text_list.append(text_dict['value'])
    transcribed_text = ' '.join(text_list)

    return transcribed_text


def transcribe_video_audio(video_path, rev_ai_key):
    """
    This function takes in the subclip video path and then transcribes it and returns the text
    """
    # Initialise a path for the output audio
    output_audio_path = '%s/temp.mp3' % os.getcwd()

    extract_mp3_audio_from_mp4_file(video_path, output_audio_path)
    
    transcribed_text = transcribe_audio_file(output_audio_path, rev_ai_key)
    
    return transcribed_text