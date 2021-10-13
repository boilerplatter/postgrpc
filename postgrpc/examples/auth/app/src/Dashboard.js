import { ClipLoader } from 'react-spinners'
import { useState } from 'react'
import { useIdentity } from './auth'
import { useNotes } from './postgres'
import Table from 'react-table'
import { v4 as uuid } from 'uuid'

// set up constants
const NO_NOTES_PROMPT = 'No notes yet! Click the "Add note" button to get started'

const Dashboard = () => {
  const [identity, logOut] = useIdentity()
  const [notes, saveNote, editNote, deleteNote] = useNotes(identity)
  const [isAddingNote, setIsAddingNote] = useState(false)
  const [editingNote, setEditingNote] = useState(null)
  const [editedNoteContents, setEditedNoteContents] = useState(null)

  // set up event handlers
  const onLogOut = async event => {
    event.preventDefault()
    await logOut()
  }

  const onAddNote = event => {
    event.preventDefault()
    setIsAddingNote(true)
    setEditingNote(uuid()) // generate throwaway id
    setEditedNoteContents(null)
  }

  const onSaveNote = async () => {
    await saveNote(editedNoteContents)
    setIsAddingNote(false)
  }

  const onEditNoteContents = event => setEditedNoteContents(event.target.value)

  const getOnEditNote = (id, note) => () => {
    setEditedNoteContents(note)
    setEditingNote(id)
  }

  const onCancelEditNote = () => {
    setIsAddingNote(false)
    setEditingNote(null)
  }

  const getOnDeleteNote = id => async () => deleteNote(id)

  const getOnUpdateNote = id => async () => {
    await editNote(id, editedNoteContents)
    setEditingNote(null)
  }

  // generate columns
  const columns = [
    {
      Header: 'Contents',
      id: 'note',
      style: {
        whiteSpace: 'unset'
      },
      accessor: ({ id, note }) => editingNote === id
        ? <textarea value={editedNoteContents} onChange={onEditNoteContents} />
        : <>{note}</>
    },
    {
      Header: 'Actions',
      id: 'actions',
      minWidth: 175,
      maxWidth: 200,
      accessor: ({ id, note }) => {
        const saveHandler = isAddingNote
          ? onSaveNote
          : getOnUpdateNote(id)

        const primaryAction = editingNote === id
          ? <button onClick={saveHandler} disabled={!editedNoteContents}>Save</button>
          : <button onClick={getOnEditNote(id, note)} disabled={isAddingNote}>Edit</button>

        const secondaryAction = editingNote === id
          ? <button className='danger' onClick={onCancelEditNote}>Cancel</button>
          : <button className='danger' onClick={getOnDeleteNote(id)}>Delete</button>

        return <span className='buttons'>
          {primaryAction}
          {secondaryAction}
        </span>
      }
    },
  ]

  // map notes to editable table rows, including new note fields
  const items = Object
    .values(notes)
    .sort((left, right) => left.created_at < right.created_at)

  const data = isAddingNote
    ? [{ id: editingNote }, ...items]
    : items

  // decide between dashboard contents
  const contents = data.length
    ? <Table
      columns={columns}
      data={data}
      minRows={0}
      showFilters={false}
      showPagination={false}
      TheadComponent={() => null} />
    : <p>{NO_NOTES_PROMPT}</p>

  return identity
    ? <>
      <h1>{identity.traits.name.first}'s Notes</h1>
      <span className='buttons'>
        <button onClick={onAddNote} disabled={isAddingNote}>Add note</button>
        <button className='danger' onClick={onLogOut}>Log out</button>
      </span>
      {contents}
    </>
    : <ClipLoader color='white' />
}

export default Dashboard
