import { useEffect, useState } from 'react'
import { ClipLoader } from 'react-spinners'
import { useIdentity } from './auth'
import { toast } from 'react-toastify'

// set up constants
const POSTGRPC_URL = 'http://0.0.0.0:8080' // FIXME: get from the environment and point to Oathkeeper instead of envoy
const NO_NOTES_PROMPT = 'No notes yet! Click the "Add note" button to get started'

// hooks for interacting with a user's notes
const useNotes = (identity) => {
  const [notes, setNotes] = useState([])

  // fetch the latest notes once on render
  useEffect(() => {
    const getNotes = async () => {
      const statement = 'SELECT id, note FROM notes'
      const body = JSON.stringify({ statement })
      const response = await fetch(
        `${POSTGRPC_URL}/query`,
        {
          body,
          method: 'POST',
          headers: {
            'X-Postgres-Role': identity.id, // FIXME: use Oathkeeper mutators instead
            'Content-Type': 'application/json'
          }
        }
      )

      const payload = await response.json()

      if (response.ok) {
        setNotes(payload)
      } else {
        toast.error(payload.message || 'Failed to fetch notes from the database')
      }
    }

    if (identity) {
      getNotes()
    }
  }, [identity])

  // add a single note
  const saveNote = async (note) => {
    const statement = `
      INSERT INTO notes (note)
      VALUES ($1)
      RETURNING id, note
    `
    const values = [note]
    const body = JSON.stringify({ statement, values })
    const response = await fetch(
      `${POSTGRPC_URL}/query`,
      {
        body,
        method: 'POST',
        headers: {
          'X-Postgres-Role': identity.id, // FIXME: use Oathkeeper mutators instead
          'Content-Type': 'application/json'
        }
      }
    )

    const payload = await response.json()

    // FIXME: figure out why fetch isn't handling statuses correctly
    if (!payload.code) {
      setNotes([...notes, ...payload])
    } else {
      toast.error(payload.message || 'Failed to insert note into the database')
    }
  }

  return [notes, saveNote]
}

const NewNote = ({ onSubmit, onCancel }) =>
  <form className='new-note' onSubmit={onSubmit} onReset={onCancel}>
    <textarea placeholder='New note here' name='note' required />
    <span className='buttons'>
      <button type='submit'>Save</button>
      <button type='reset' className='danger'>Cancel</button>
    </span>
  </form>

const Dashboard = () => {
  const [identity, logOut] = useIdentity()
  const [notes, saveNote] = useNotes(identity)
  const [isAddingNote, setIsAddingNote] = useState(false)

  // set up event handlers
  const onLogOut = async event => {
    event.preventDefault()
    await logOut()
  }

  const onAddNote = event => {
    event.preventDefault()
    setIsAddingNote(true)
  }

  const onCancelNote = event => {
    event.preventDefault()
    setIsAddingNote(false)
  }

  const onSaveNote = async event => {
    event.preventDefault()
    const form = new FormData(event.target)
    const note = form.get('note')
    await saveNote(note)
  }

  // map notes to editable list items
  const items = notes.map(({ id, note }) => <li key={id}>{note}</li>)

  // conditionally render the new note form
  const newNoteForm =
    isAddingNote
      ? <NewNote onCancel={onCancelNote} onSubmit={onSaveNote} />
      : null

  // decide between dashboard contents
  const contents = items.length
    ? <>
      {newNoteForm}
      <ul>{items}</ul>
    </>
    : newNoteForm || <p>{NO_NOTES_PROMPT}</p>

  return identity
    ? <>
      <h1>{identity.traits.name.first}'s Notes</h1>
      <span className='buttons'>
        <button onClick={onAddNote}>Add note</button>
        <button className='danger' onClick={onLogOut}>Log out</button>
      </span>
      {contents}
    </>
    : <ClipLoader color='white' />
}

export default Dashboard
