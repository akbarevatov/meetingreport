import json
import pandas as pd


def create_dim_comm_type(df):
    unique_comm_types = df['comm_type'].dropna().unique()
    dim_comm_type = pd.DataFrame({
        'comm_type_id': range(1, len(unique_comm_types) + 1),
        'comm_type': unique_comm_types
    })
    return dim_comm_type

def create_dim_subject(df):
    unique_subjects = df['subject'].dropna().unique()
    dim_subject = pd.DataFrame({
        'subject_id': range(1, len(unique_subjects) + 1),
        'subject': unique_subjects
    })
    return dim_subject

def create_dim_user(df):
    def extract_users(df):
        def get_users(json_str):
            try:
                data = json.loads(json_str)
                return data.get("meeting_attendees")
            except (json.JSONDecodeError, TypeError):
                return None

        users = df['raw_content'].apply(get_users).dropna()
        df_user = pd.DataFrame({"users": users})
        return df_user

    df_exploded = extract_users(df).explode('users')

    df_users = pd.DataFrame({
        "user_id" : range(1, len(df_exploded) + 1),
        "name" : df_exploded["users"].apply(lambda x: x.get("name")),
        "email" : df_exploded["users"].apply(lambda x: x.get("email")),
        "location" : df_exploded["users"].apply(lambda x: x.get("location")),
        "displayName" : df_exploded["users"].apply(lambda x: x.get("displayName")),
        "phoneNumber" : df_exploded["users"].apply(lambda x: x.get("phoneNumber"))

    })
    return df_users

def create_dim_calendar(df):
    def get_calendar_id(json_str):
        try:
            data = json.loads(json_str)
            return data.get("calendar_id")
        except (json.JSONDecodeError, TypeError):
            return None

    df_calendar_id = df['raw_content'].apply(get_calendar_id).dropna().unique()
    
    dim_calendar = pd.DataFrame({
        "calendar_id" : range(1, len(df_calendar_id) + 1),
        "raw_calendar_id" : df_calendar_id
    })
    return dim_calendar

def create_dim_audio(df):
    def get_audio_url(json_str):
        try:
            data = json.loads(json_str)
            return data.get("audio_url")
        except (json.JSONDecodeError, TypeError):
            return None

    df_audio_url = df['raw_content'].apply(get_audio_url).dropna().unique()
    
    dim_audio = pd.DataFrame({
        "audio_id" : range(1, len(df_audio_url) + 1),
        "raw_audio_url" : df_audio_url
    })
    return dim_audio

def create_dim_video(df):
    def get_video_url(json_str):
        try:
            data = json.loads(json_str)
            return data.get("video_url")
        except (json.JSONDecodeError, TypeError):
            return None

    df_video_url = df['raw_content'].apply(get_video_url).dropna().unique()
    
    dim_video = pd.DataFrame({
        "video_id" : range(1, len(df_video_url) + 1),
        "raw_video_url" : df_video_url
    })
    return dim_video

def create_dim_transcript(df):
    def get_transcript_url(json_str):
        try:
            data = json.loads(json_str)
            return data.get("transcript_url")
        except (json.JSONDecodeError, TypeError):
            return None

    df_transcript_url = df['raw_content'].apply(get_transcript_url).dropna().unique()
    
    dim_transcript = pd.DataFrame({
        "transcript_id" : range(1, len(df_transcript_url) + 1),
        "raw_transcript_url" : df_transcript_url
    })
    return dim_transcript

def safe_parse(json_str):
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    
def extract_keys(df):
    content = df["raw_content"].apply(safe_parse)

    df["raw_id"] = content.apply(lambda x: x.get("id"))
    df["raw_title"] = content.apply(lambda x: x.get("title"))
    df["raw_audio_url"] = content.apply(lambda x: x.get("audio_url"))
    df["raw_video_url"] = content.apply(lambda x: x.get("video_url"))
    df["raw_transcript_url"] = content.apply(lambda x: x.get("transcript_url"))
    df["raw_calendar_id"] = content.apply(lambda x: x.get("calendar_id"))
    df["raw_datetime"] = content.apply(lambda x: x.get("dateString"))  
    df["raw_duration"] = content.apply(lambda x: x.get("duration"))

    return df

def create_fact_communication(df, dims):
    df = extract_keys(df)

    # Join with dimensions
    df = df.merge(dims['dim_comm_type'], how='left', on='comm_type')
    df = df.merge(dims['dim_calendar'], how='left', on='raw_calendar_id')
    df = df.merge(dims['dim_audio'], how='left', on='raw_audio_url')
    df = df.merge(dims['dim_video'], how='left', on='raw_video_url')
    df = df.merge(dims['dim_transcript'], how='left', on='raw_transcript_url')
    df = df.merge(dims['dim_subject'], how='left', on='subject')
    


    fact = df[[
        'source_id',
        'raw_id',
        'comm_type_id',
        'subject_id',
        'calendar_id',
        'audio_id',
        'video_id',
        'transcript_id',
        'raw_datetime',  
        'ingested_at',
        'processed_at',
        'is_processed',
        'raw_title',
        'raw_duration',
    ]].copy()

    fact.insert(0, 'comm_id', range(1, len(fact) + 1))
    fact.rename(columns={'raw_datetime': 'datetime_id'}, inplace=True)
    return fact

def create_bridge_comm_user(df, dim_user):
    parsed_content = df["raw_content"].apply(safe_parse)

    bridge_rows = []

    for idx, row in df.iterrows():
        content = parsed_content[idx]
        comm_id = idx+1

        attendees = content.get("meeting_attendees", [])
        participants = content.get("participants", [])
        speakers = [s.get("name") for s in content.get("speakers", []) if s.get("name")]
        organiser = content.get("organizer_email")

        for user_idx, user_row in dim_user.iterrows():
            user_id = user_row["user_id"]
            user_email = user_row["email"]
            user_name = user_row["name"]

            is_attendee = any(a.get("email") == user_email for a in attendees)
            is_participant = user_email in participants
            is_speaker = user_name in speakers
            is_organiser = user_email==organiser

            if is_attendee or is_organiser or is_participant or is_speaker:
                bridge_rows.append({
                    "comm_id": comm_id,
                    "user_id": user_id,
                    "isAttendee": is_attendee,
                    "isParticipant": is_participant,
                    "isSpeaker": is_speaker,
                    "isOrganiser": is_organiser
                })
    
    bridge_comm_user = pd.DataFrame(bridge_rows)
    return bridge_comm_user

def main():
    df = pd.read_excel("raw_data.xlsx")
    
    dim_comm_type = create_dim_comm_type(df)
    dim_subject = create_dim_subject(df)
    dim_user = create_dim_user(df)
    dim_calendar = create_dim_calendar(df)
    dim_audio = create_dim_audio(df)
    dim_video = create_dim_video(df)
    dim_transcript = create_dim_transcript(df)

    dims = {
        "dim_comm_type": dim_comm_type,
        "dim_subject": dim_subject,
        "dim_user": dim_user,
        "dim_calendar": dim_calendar,
        "dim_audio": dim_audio,
        "dim_video": dim_video,
        "dim_transcript": dim_transcript
    }

    fact_communication = create_fact_communication(df=df, dims=dims)
    bridge_comm_user = create_bridge_comm_user(df=df, dim_user=dim_user)

    with pd.ExcelWriter("final_data.xlsx") as writer:
        dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
        dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
        dim_user.to_excel(writer, sheet_name="dim_user", index=False)
        dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
        dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
        dim_video.to_excel(writer, sheet_name="dim_video", index=False)
        dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
        fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
        bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)

if __name__=="__main__":
    main()

