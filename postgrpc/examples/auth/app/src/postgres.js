import { useEffect, useState } from 'react'
import { toast } from 'react-toastify'

// set up constants
const REACT_APP_POSTGRPC_URL = process.env.REACT_APP_POSTGRPC_URL

// wrap fetch in a unified query interface
const query = async (statement, values = []) => {
  const body = JSON.stringify({ statement, values })

  const response = await fetch(
    `${REACT_APP_POSTGRPC_URL}/query`,
    {
      body,
      credentials: 'include',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    }
  )

  return response.json()
}

// hooks for interacting with a user's notes
export const useNotes = (identity) => {
  const [notes, setNotes] = useState({})

  // fetch the latest notes once on render
  useEffect(() => {
    const getNotes = async () => {
      const statement = `
        SELECT id, note, created_at
        FROM notes
      `
      const response = await query(statement)

      if (!response.code) {
        const newNotes = response.reduce((notes, note) => ({
          ...notes,
          [note.id]: note
        }), {})

        setNotes(newNotes)
      } else {
        toast.error(response.message || 'Failed to fetch notes from the database')
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
      RETURNING id, note, created_at
    `
    const values = [note]
    const response = await query(statement, values)

    if (!response.code) {
      const [newNote] = response

      setNotes({
        ...notes,
        [newNote.id]: newNote
      })
    } else {
      toast.error(response.message || 'Failed to insert note into the database')
    }
  }

  // edit a note by ID
  const editNote = async (id, note) => {
    const statement = `
      UPDATE notes
      SET note = $2
      WHERE id = $1
      RETURNING id, note, created_at
    `
    const values = [id, note]
    const response = await query(statement, values)

    if (!response.code) {
      const [newNote] = response

      setNotes({
        ...notes,
        [newNote.id]: newNote
      })
    } else {
      toast.error(response.message || 'Failed to insert note into the database')
    }
  }

  // delete a note by ID
  const deleteNote = async (id) => {
    const statement = `
      DELETE FROM notes
      WHERE id = $1
    `
    const values = [id]
    const response = await query(statement, values)

    if (!response.code) {
      const newNotes = Object
        .entries(notes)
        .filter(([key]) => key !== id)
        .reduce((notes, [key, value]) => ({
          ...notes,
          [key]: value
        }), {})

      setNotes(newNotes)
    } else {
      toast.error(response.message || 'Failed to delete note from the database')
    }
  }

  return [notes, saveNote, editNote, deleteNote]
}
